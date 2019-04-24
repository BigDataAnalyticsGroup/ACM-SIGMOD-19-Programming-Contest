//===== main.cpp =======================================================================================================
//
//  Author: Immanuel Haffner <haffner.immanuel@gmail.com>
//
//  Licence:
//      Copyright 2019 Immanuel Haffner
//
//      Licensed under the Apache License, Version 2.0 (the "License");
//      you may not use this file except in compliance with the License.
//      You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//      Unless required by applicable law or agreed to in writing, software
//      distributed under the License is distributed on an "AS IS" BASIS,
//      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//      See the License for the specific language governing permissions and
//      limitations under the License.
//
//  Description:
//      This file provides the main method for sorting.  A preprocessor flag is used to compile as partitioning or
//      sorting binary.
//
//======================================================================================================================

#ifdef SUBMISSION
#warning "Compiling submission build"
#endif

#include "mmap.hpp"
#include "record.hpp"
#include "sort.hpp"
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <err.h>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <sched.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>

#ifdef WITH_PCM
#include "cpucounters.h"
#endif

#define READ_CONCURRENT 1


namespace ch = std::chrono;
using namespace std::chrono_literals;

constexpr std::size_t FILE_SIZE_SMALL   = 00UL * 1000 * 1000 * 1000; // 10 GB
constexpr std::size_t FILE_SIZE_MEDIUM  = 20UL * 1000 * 1000 * 1000; // 20 GB
//constexpr std::size_t FILE_SIZE_MEDIUM  = 00UL * 1000 * 1000 * 1000; // 20 GB
constexpr std::size_t FILE_SIZE_LARGE   = 60UL * 1000 * 1000 * 1000; // 60 GB
constexpr std::size_t NUM_BLOCKS_PER_SLAB = 1024;

#ifdef SUBMISSION
constexpr unsigned NUM_THREADS_READ = 20;
constexpr unsigned NUM_THREADS_PARTITION = 10;
constexpr const char * const OUTPUT_PATH = "/output-disk";
constexpr std::size_t IN_MEMORY_BUFFER_SIZE = 26UL * 1024 * 1024 * 1024; // 26 GiB
#else
constexpr unsigned NUM_THREADS_READ = 4;
constexpr unsigned NUM_THREADS_PARTITION = 6;
constexpr const char * const OUTPUT_PATH = "./buckets";
constexpr std::size_t IN_MEMORY_BUFFER_SIZE = 2UL * 1024 * 1024 * 1024; // 2 GiB
#endif


/** Information for the threads. */
struct thread_info
{
    int fd; ///< file descriptor of the file
    const char *path; ///< the path to the file to read from / write to
    std::size_t offset; ///< offset from start of the file in bytes
    std::size_t count; ///< number of bytes to read from / write to the file
    void *buffer; ///< I/O buffer to read into / write out
    std::size_t slab_size; ///< the size of a slab, the granule at which I/O is performed; a multiple of the preferred block size
};

void readall(int fd, void *buf, std::size_t count, off_t offset)
{
    uint8_t *dst = reinterpret_cast<uint8_t*>(buf);
    while (count) {
        const int bytes_read = pread(fd, dst, count, offset);
        if (bytes_read == -1)
            err(EXIT_FAILURE, "Could not read from fd %d", fd);
        count -= bytes_read;
        offset += bytes_read;
        dst += bytes_read;
    }
}

/** Read from fd count many bytes, starting at offset, and write them to buf. */
void read_concurrent(int fd, void *buf, std::size_t count, off_t offset)
{
    uint8_t *dst = reinterpret_cast<uint8_t*>(buf);
    struct stat st;
    if (fstat(fd, &st))
        err(EXIT_FAILURE, "Could not get status of fd %d", fd);
    const std::size_t slab_size = NUM_BLOCKS_PER_SLAB * st.st_blksize;

    if (count < 2 * slab_size) {
        readall(fd, dst, count, offset);
    } else {
        /* Read unaligned start. */
        off_t unaligned = slab_size - (offset % slab_size);
        if (unaligned) {
            readall(fd, dst, unaligned, offset);
            count -= unaligned;
            dst += unaligned;
            offset += unaligned;
        }

        /* Concurrently read the rest. */
        std::array<std::thread, NUM_THREADS_READ> threads;
        const std::size_t num_slabs = std::ceil(double(count) / slab_size);
        const std::size_t num_slabs_per_thread = num_slabs / NUM_THREADS_READ;
        const std::size_t count_per_thread = num_slabs_per_thread * slab_size;
        for (unsigned tid = 0; tid != NUM_THREADS_READ; ++tid) {
            const std::size_t thread_count = tid == NUM_THREADS_READ - 1 ? count - (NUM_THREADS_READ - 1) * count_per_thread : count_per_thread;
            threads[tid] = std::thread(readall, fd, dst, thread_count, offset);
            dst += thread_count;
            offset += thread_count;
        }
        for (auto &t : threads)
            t.join();
    }
}

int main(int argc, const char **argv)
{
    if (argc != 3) {
        std::cerr << "USAGE:\n\t" << argv[0] << " <INPUT-FILE> <OUTPUT-FILE>" << std::endl;
        std::exit(EXIT_FAILURE);
    }

#ifdef WITH_PCM
    PCM &the_PCM = *PCM::getInstance(); // initialize singleton
#endif

#ifdef SUBMISSION
    /* Disable synchronization with C stdio. */
    std::ios::sync_with_stdio(false);
#endif

    /* Open input file and get file stats. */
    int fd_in = open(argv[1], O_RDONLY);
    if (fd_in == -1)
        err(EXIT_FAILURE, "Could not open file '%s'", argv[1]);
    struct stat stat_in;
    if (fstat(fd_in, &stat_in))
        err(EXIT_FAILURE, "Could not get status of file '%s'", argv[1]);
    const std::size_t size_in_bytes = std::size_t(stat_in.st_size);
    const std::size_t num_records = size_in_bytes / sizeof(record);
    std::cerr << "Input file \"" << argv[1] << "\" is " << size_in_bytes << " bytes large, contains "
              << num_records << " records in blocks of size " << stat_in.st_blksize << " bytes.\n";

    /* Open output file and allocate sufficient space. */
    int fd_out = open(argv[2], O_CREAT|O_TRUNC|O_RDWR, 0644);
    if (fd_out == -1)
        err(EXIT_FAILURE, "Could not open file '%s'", argv[2]);
    if (fallocate(fd_out, /* mode = */ 0, /* offset= */ 0, /* len= */ size_in_bytes)) {
        if (errno == EOPNOTSUPP) {
            if (ftruncate(fd_out, size_in_bytes))
                err(EXIT_FAILURE, "Could not truncate file '%s' to size %lu", argv[2], size_in_bytes);
        } else {
            err(EXIT_FAILURE, "Could not allocate space for output file '%s'", argv[2]);
        }
    }

    /* MMap the output file. */
    void *output = mmap(/* addr=   */ nullptr,
                        /* length= */ size_in_bytes,
                        /* prot=   */ PROT_READ|PROT_WRITE,
                        /* flags=  */ MAP_SHARED,
                        /* fd=     */ fd_out,
                        /* offset= */ 0);
    if (output == MAP_FAILED)
        err(EXIT_FAILURE, "Could not map output file '%s' into memory", argv[2]);
    std::cerr << "Memory map the output file at virtual address " << output << ".\n";

    /* Choose the algorithm based on the file size. */
    if (size_in_bytes <= FILE_SIZE_SMALL)
    {
        /*----- SMALL DATA SET ---------------------------------------------------------------------------------------*/
        std::cerr << "Detected SMALL data set\n";
        std::cerr << "Not yet supported.\n";
        std::exit(EXIT_FAILURE);
    }
    else if (size_in_bytes <= FILE_SIZE_MEDIUM)
    {
        /*----- MEDIUM DATA SET --------------------------------------------------------------------------------------*/
        std::cerr << "Detected MEDIUM data set\n";

        const auto t_begin_read = ch::high_resolution_clock::now();

        /* Spawn threads to concurrently read file. */
        std::cerr << "Read data into main memory.\n";
        read_concurrent(fd_in, output, size_in_bytes, 0);

        const auto t_begin_sort = ch::high_resolution_clock::now();

        /* Sort the records. */
        {
            record *records = reinterpret_cast<record*>(output);

#ifndef NDEBUG
            if (num_records <= 20) {
                for (auto p = records, end = records + num_records; p != end; ++p)
                    p->to_ascii(std::cerr);
            }
#endif

            std::cerr << "Sort the data.\n";
            american_flag_sort_parallel(records, records + num_records, 0);
            assert(std::is_sorted(records, records + num_records));
        }

        const auto t_begin_write = ch::high_resolution_clock::now();

        /* Write the sorted data to the output file. */
        std::cerr << "Write sorted data back to disk.\n";
        msync(output, size_in_bytes, MS_ASYNC);
        munmap(output, size_in_bytes);

        const auto t_finish = ch::high_resolution_clock::now();

        /* Report times and throughput. */
        {
            constexpr unsigned long MiB = 1024 * 1024;

            const auto d_read_s = ch::duration_cast<ch::milliseconds>(t_begin_sort - t_begin_read).count() / 1e3;
            const auto d_sort_s = ch::duration_cast<ch::milliseconds>(t_begin_write - t_begin_sort).count() / 1e3;
            const auto d_write_s = ch::duration_cast<ch::milliseconds>(t_finish - t_begin_write).count() / 1e3;

            const auto throughput_read_mbs = size_in_bytes / MiB / d_read_s;
            const auto throughput_sort_mbs = size_in_bytes / MiB / d_sort_s;
            const auto throughput_write_mbs = size_in_bytes / MiB / d_write_s;

            std::cerr << "read: " << d_read_s << " s (" << throughput_read_mbs << " MiB/s)\n"
                      << "sort: " << d_sort_s << " s (" << throughput_sort_mbs << " MiB/s)\n"
                      << "write: " << d_write_s << " s (" << throughput_write_mbs << " MiB/s)\n";
        }

#ifdef SUBMISSION
        std::this_thread::sleep_for(10s);
#endif
    }
    else
    {
        /*----- LARGE DATA SET ---------------------------------------------------------------------------------------*/
        std::cerr << "LARGE data set\n";

        /* Idea:
         * Read the first 30+x GB of data, partition on the fly and write to buckets on disk, where each bucket is a
         * separate file.
         * Read the remaining 30-x GB of data into RAM, fully sort.  Read bucket by bucket from disk, sort, merge with
         * in-memory data and write out to mmaped output file.  (Make sure to eagerly mark written pages for eviction.)
         * Again, we should be able to save the write of the final 30GB because of mmap; the kernel will do this for us.
         */

        const std::size_t num_records_to_sort = std::min(size_in_bytes, IN_MEMORY_BUFFER_SIZE) / sizeof(record);
        const std::size_t num_bytes_to_sort = num_records_to_sort * sizeof(record);
        const std::size_t num_records_to_partition = size_in_bytes <= IN_MEMORY_BUFFER_SIZE ? 0 :
            num_records - num_records_to_sort;
        const std::size_t num_bytes_to_partition = num_records_to_partition * sizeof(record);
        assert(num_records_to_partition < num_records);
        assert(num_bytes_to_sort <= size_in_bytes);
        assert(num_bytes_to_partition < size_in_bytes);
        assert(num_records_to_sort + num_records_to_partition == num_records);
        assert(num_bytes_to_sort + num_bytes_to_partition == size_in_bytes);

        const auto t_begin_read = ch::high_resolution_clock::now();

        /* Concurrently read the first part of the file. */
        std::cerr << "Read the first " << num_records_to_sort << " records (" << num_bytes_to_sort << " bytes).\n";
        void *in_memory_buffer = mmap(nullptr, num_bytes_to_sort, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS,
                                      /* fd= */ -1, 0);
        if (in_memory_buffer == MAP_FAILED)
            err(EXIT_FAILURE, "Failed to map temporary read buffer");
        read_concurrent(fd_in, in_memory_buffer, num_bytes_to_sort, 0);
#ifndef NDEBUG
        std::cerr << "Allocated the in-memory buffer in range " << (void*)(in_memory_buffer) << " to "
                  << (void*)(reinterpret_cast<uint8_t*>(in_memory_buffer) + num_bytes_to_sort) << ".\n";
#endif

        ch::high_resolution_clock::time_point t_begin_sort, t_end_sort;

        /* Sort the records in-memory. */
        std::cerr << "Sort " << num_records_to_sort << " records in-memory in a separate thread.\n";
        std::thread thread_sort = std::thread([in_memory_buffer, num_records_to_sort, &t_begin_sort, &t_end_sort]() {
            record * const records = reinterpret_cast<record*>(in_memory_buffer);
            t_begin_sort = ch::high_resolution_clock::now();
            american_flag_sort_parallel(records, records + num_records_to_sort, 0);
            assert(std::is_sorted(records, records + num_records_to_sort));
            t_end_sort = ch::high_resolution_clock::now();
        });

        const auto t_begin_partition = ch::high_resolution_clock::now();

        /* Create the output files for the buckets. */
        struct bucket_t {
            FILE *file; ///< the associated stream object
            void *buffer; ///< the buffer assigned to the stream object
            std::atomic_uint_fast64_t size = 0UL; ///< the size of the bucket in bytes
            void *addr = nullptr; ///< the address of the mapped memory region
            std::thread sorter; ///< the thread that sorts the bucket
        };
        std::array<bucket_t, NUM_BUCKETS> buckets;
        {
            std::ostringstream path;
            for (std::size_t bucket_id = 0; bucket_id != NUM_BUCKETS; ++bucket_id) {
                path.str("");
                path << OUTPUT_PATH << "/bucket_" << std::setfill('0') << std::setw(3) << bucket_id << ".bin";
                FILE *file = fopen(path.str().c_str(), "w+b");
                if (not file)
                    err(EXIT_FAILURE, "Could not open bucket file '%s'", path.str().c_str());

                /* Assign custom, large buffer to file stream. */
                const std::size_t buffer_size = 256 * stat_in.st_blksize;
                void *buffer = malloc(buffer_size);
                if (not buffer)
                    err(EXIT_FAILURE, "Failed to allocate I/O buffer");
                if (setvbuf(file, static_cast<char*>(buffer), _IOFBF, buffer_size))
                    err(EXIT_FAILURE, "Failed to set custom buffer for file");

                buckets[bucket_id].file = file;
                buckets[bucket_id].buffer = buffer;
            }
        }

        /* Read second part and partition. */
        if (num_records_to_partition) {
            std::cerr << "Read and partition the remaining " << num_records_to_partition << " records ("
                      << num_bytes_to_partition << " bytes), starting at offset " << num_bytes_to_sort << ".\n";

            /* Partition concurrently by evenly dividing the input between multiple partitioning threads.  Every thread
             * owns a buffer of N records for each bucket.  Read the input and append each record to the buffer of its
             * destination bucket.  If a buffer runs full, flush it to the bucket file. */
            auto partition = [fd_in, argv, &buckets] (std::size_t offset, std::size_t count) {
                constexpr std::size_t NUM_RECORDS_READ = 10000;
                constexpr std::size_t NUM_RECORDS_PER_BUFFER = 1024;

                auto read_buffer = new record[NUM_RECORDS_READ]; ///< buffer to read into from input file
                using buffer_t = std::array<record, NUM_RECORDS_PER_BUFFER>;
                auto buffers = new buffer_t[NUM_BUCKETS]; ///< write-back buffer for each bucket file
                std::array<record*, NUM_BUCKETS> heads; ///< next empty slot in each write-back buffer

                for (std::size_t bucket_id = 0; bucket_id != NUM_BUCKETS; ++bucket_id)
                    heads[bucket_id] = buffers[bucket_id].data();

                /* Read as many records at once as fit into our buffer. */
                while (count >= NUM_RECORDS_READ) {
                    const auto bytes_read = pread(fd_in, read_buffer, NUM_RECORDS_READ * sizeof(record), offset);
                    if (bytes_read != NUM_RECORDS_READ * sizeof(record)) {
                        warn("Failed to read next records from file '%s' at offset %ld", argv[1], offset);
                        return;
                    }
                    offset += bytes_read;
                    count -= NUM_RECORDS_READ;

                    /* Partition all records from the read buffer into the write-back buffers. */
                    for (auto r = read_buffer, end = read_buffer + NUM_RECORDS_READ; r != end; ++r) {
                        const unsigned bucket_id = r->key[0]; // get bucket id
                        *heads[bucket_id]++ = *r; // append to write-back buffer
                        /* If the write-back buffer is full, write it out and reset head. */
                        if (heads[bucket_id] == &buffers[bucket_id][NUM_RECORDS_PER_BUFFER]) {
                            heads[bucket_id] = buffers[bucket_id].data(); // reset head
                            auto &bucket = buckets[bucket_id];
                            auto written = fwrite(heads[bucket_id], sizeof(record), NUM_RECORDS_PER_BUFFER, bucket.file);
                            if (written != NUM_RECORDS_PER_BUFFER)
                                err(EXIT_FAILURE, "Failed to write all records of buffer to file");
                        }
                    }
                }
                assert(count < NUM_RECORDS_READ);

                /* Read remaining records at the end. */
                if (count) {
                    const auto bytes_read = pread(fd_in, read_buffer, count * sizeof(record), offset);
                    if (bytes_read != int(count * sizeof(record))) {
                        warn("Failed to read next records from file '%s' at offset %ld", argv[1], offset);
                        return;
                    }

                    for (auto r = read_buffer, end = read_buffer + count; r != end; ++r) {
                        const unsigned bucket_id = r->key[0];
                        *heads[bucket_id]++ = *r;
                        if (heads[bucket_id] == &buffers[bucket_id][NUM_RECORDS_PER_BUFFER]) {
                            heads[bucket_id] = buffers[bucket_id].data(); // reset head
                            auto &bucket = buckets[bucket_id];
                            auto written = fwrite(heads[bucket_id], sizeof(record), NUM_RECORDS_PER_BUFFER, bucket.file);
                            if (written != NUM_RECORDS_PER_BUFFER)
                                err(EXIT_FAILURE, "Failed to write all records of buffer to file");
                        }
                    }
                }

                /* Write-back the records still in the buffer. */
                for (std::size_t bucket_id = 0; bucket_id != NUM_BUCKETS; ++bucket_id) {
                    buffer_t &buffer = buffers[bucket_id];
                    auto head = heads[bucket_id];
                    if (head != buffer.data()) {
                        auto &bucket = buckets[bucket_id];
                        fwrite(buffer.data(), sizeof(record), head - buffer.data(), bucket.file);
                    }
                }

                delete[] read_buffer;
                delete[] buffers;
            };

#ifdef SUBMISSION
            /* Explicitly bind the partitioning to logical cores on NUMA region 1, where it can operate independently of
             * sorting. */
            {
                cpu_set_t *cpus = CPU_ALLOC(40);
                if (not cpus)
                    err(EXIT_FAILURE, "Failed to allocate CPU_SET of 40 CPUs");
                const auto size = CPU_ALLOC_SIZE(40);
                CPU_ZERO_S(size, cpus);
                for (int cpu = 10; cpu != 20; ++cpu)
                    CPU_SET_S(cpu, size, cpus);
                for (int cpu = 30; cpu != 40; ++cpu)
                    CPU_SET_S(cpu, size, cpus);
                assert(CPU_COUNT_S(size, cpus) == 20 and "allocated incorrect number of logical CPUs");

                /* Bind process and all children to the desired logical CPUs. */
                sched_setaffinity(0 /* this thread */, size, cpus);

                CPU_FREE(cpus);
            }
#endif

            std::array<std::thread, NUM_THREADS_PARTITION> threads;
            const std::size_t count_per_thread = num_records_to_partition / NUM_THREADS_PARTITION;
            std::size_t offset = num_records_to_sort;
            for (unsigned tid = 0; tid != NUM_THREADS_PARTITION; ++tid) {
                const std::size_t count = tid == NUM_THREADS_PARTITION - 1 ? num_records - offset : count_per_thread;
                threads[tid] = std::thread(partition, offset * sizeof(record), count);
                offset += count;
            }
            for (unsigned tid = 0; tid != NUM_THREADS_PARTITION; ++tid)
                threads[tid].join();

#ifdef SUBMISSION
            /* Set back to *all* logical cores again. */
            {
                cpu_set_t *cpus = CPU_ALLOC(40);
                if (not cpus)
                    err(EXIT_FAILURE, "Failed to allocate CPU_SET of 40 CPUs");
                const auto size = CPU_ALLOC_SIZE(40);
                CPU_ZERO_S(size, cpus);
                for (int cpu = 0; cpu != 40; ++cpu)
                    CPU_SET_S(cpu, size, cpus);
                assert(CPU_COUNT_S(size, cpus) == 40 and "allocated incorrect number of logical CPUs");

                /* Bind process and all children to the desired logical CPUs. */
                sched_setaffinity(0 /* this thread */, size, cpus);

                CPU_FREE(cpus);
            }
#endif
        }

        const auto t_end_partition = ch::high_resolution_clock::now();
        thread_sort.join();
        const auto t_merge_begin = ch::high_resolution_clock::now();

        /* For each partition, read it, sort it, merge with sorted records, and write out to output file. */
        std::cerr << "Merge the sorted " << num_records_to_sort << " records in memory with the buckets.\n";
        auto p_out = reinterpret_cast<record*>(output);
        auto p_sorted = reinterpret_cast<record*>(in_memory_buffer);
        auto sorted_end = p_sorted + num_records_to_sort;
        assert(std::is_sorted(p_sorted, sorted_end) and "in-memory data is not sorted");

#ifdef WITH_PCM
        auto pcm_err = the_PCM.program();
        if (pcm_err != PCM::Success)
            errx(EXIT_FAILURE, "Failed to program PCM");

        struct AllCounterState
        {
            SystemCounterState system;
            std::vector<SocketCounterState> sockets;
            std::vector<CoreCounterState> cores;
        } pcm_before, pcm_after;

        the_PCM.getAllCounterStates(pcm_before.system, pcm_before.sockets, pcm_before.cores);
#endif

        ch::high_resolution_clock::duration d_mmap_total(0), d_sort_total(0), d_merge_total(0);

        for (std::size_t i = 0; i != NUM_BUCKETS + 2; ++i) {
#ifndef NDEBUG
            std::cerr << "i = " << i << ":\n";
#endif

            /* MMap and fetch the next bucket. */
            if (i < NUM_BUCKETS) {
                const auto t_mmap_begin = ch::high_resolution_clock::now();
                const auto bucket_id = i;
                assert(bucket_id < NUM_BUCKETS);
                auto &bucket = buckets[bucket_id];
                if (fflush(bucket.file))
                    err(EXIT_FAILURE, "Failed to flush bucket %lu", bucket_id);
                int fd = fileno(bucket.file);
                struct stat status;
                if (fstat(fd, &status))
                    err(EXIT_FAILURE, "Failed to get status of bucket %lu file", bucket_id);
                bucket.size = status.st_size;

                if (bucket.size) {
                    /* Get the bucket data into memory. */
                    assert(bucket.addr == nullptr);
                    bucket.addr = mmap(nullptr, bucket.size, PROT_READ|PROT_WRITE, MAP_PRIVATE, fd, 0);
                    if (bucket.addr == MAP_FAILED)
                        err(EXIT_FAILURE, "Failed to mmap bucket %lu file", bucket_id);
                    if (madvise(bucket.addr, bucket.size, MADV_WILLNEED))
                        warn("Failed to advise access to the bucket file");
                    __builtin_prefetch(bucket.addr);
                }
                const auto t_mmap_end = ch::high_resolution_clock::now();
                d_mmap_total += t_mmap_end - t_mmap_begin;
                std::cerr << "MMap bucket took "
                          << ch::duration_cast<ch::nanoseconds>(t_mmap_end - t_mmap_begin).count() / 1e6 << " ms\n";
            }

            const auto t_sort_bucket_begin = ch::high_resolution_clock::now();
            if (i >= 1 and i <= NUM_BUCKETS) {
                const auto bucket_id = i - 1;
                assert(bucket_id < NUM_BUCKETS);
                auto &bucket = buckets[bucket_id];

                if (bucket.size) {
                    record *records = reinterpret_cast<record*>(bucket.addr);
                    const std::size_t num_records_in_bucket = bucket.size / sizeof(record);
                    bucket.sorter = std::thread(american_flag_sort_parallel,
                                                records, records + num_records_in_bucket, 1);
                }
            }

            if (i >= 2) {
                const auto bucket_id = i - 2;
                assert(bucket_id < NUM_BUCKETS);
                auto &bucket = buckets[bucket_id];

                if (bucket.size) {
                    bucket.sorter.join(); // wait for the sorter thread to finish sorting the bucket

                    const auto t_merge_bucket_begin = ch::high_resolution_clock::now();
                    d_sort_total += t_merge_bucket_begin - t_sort_bucket_begin;

                    std::cerr << "  Sort bucket took "
                              << ch::duration_cast<ch::nanoseconds>(t_merge_bucket_begin - t_sort_bucket_begin).count() / 1e6
                              << " ms\n";

                    const std::size_t num_records_in_bucket = bucket.size / sizeof(record);
                    const auto bucket_begin = reinterpret_cast<record*>(bucket.addr);
                    const auto bucket_end = bucket_begin + num_records_in_bucket;
                    assert(std::is_sorted(bucket_begin, bucket_end));

                    auto p_bucket = bucket_begin;
                    const auto p_sorted_old = p_sorted;
                    const auto p_out_old = p_out;

#ifndef NDEBUG
                    std::cerr << "  Merge bucket " << bucket_id << " of " << num_records_in_bucket
                              << " sorted records.\n";
#endif
                    /* If the in-memory data is not yet finished, merge. */
                    if (p_sorted != sorted_end) {
                        /* Merge this bucket with the sorted data. */
                        while (p_bucket != bucket_end and p_sorted != sorted_end) {
                            //IACA_START;
                            assert(p_bucket <= bucket_end);
                            assert(p_sorted <= sorted_end);
                            assert(p_out < reinterpret_cast<record*>(output) + num_records);

#if 1
                            const bool less = *p_bucket < *p_sorted;
                            *p_out++ = less ? *p_bucket++ : *p_sorted++;
#else
                            *p_out++ = *p_bucket < *p_sorted ? *p_bucket++ : *p_sorted++;
#endif
                        }
                        //IACA_END;
                        assert(p_out - p_out_old == (p_sorted - p_sorted_old) + (p_bucket - bucket_begin) and
                                "incorrect number of elements written to output");
                        assert(std::is_sorted(p_out_old, p_out) and "output not sorted");
                        assert ((p_bucket == bucket_end) != (p_sorted == sorted_end) and
                                "either the bucket is finished and sorted in-memory data remains or vice versa");
                    } else {
#ifndef NDEBUG
                        std::cerr << "  In-memory sorted data is already merged.  Flush bucket to output.\n";
#endif
                    }

                    /* Finish the bucket. */
                    while (p_bucket != bucket_end)
                        *p_out++ = *p_bucket++;

                    assert(p_out - p_out_old == (p_sorted - p_sorted_old) + (p_bucket - bucket_begin) and
                            "incorrect number of elements written to output");
                    assert(std::is_sorted(p_out_old, p_out) and "output not sorted");

                    const auto t_merge_bucket_end = ch::high_resolution_clock::now();
                    d_merge_total += t_merge_bucket_end - t_merge_bucket_begin;

                    std::cerr
                        << "  Merge bucket of " << double(bucket.size) / (1024 * 1024) << " MiB into a total of "
                        << double((p_out - p_out_old) * sizeof(record)) / (1024 * 1024) << " MiB took "
                        << ch::duration_cast<ch::nanoseconds>(t_merge_bucket_end - t_merge_bucket_begin).count() / 1e6
                        << " ms\n";

                    /* Release resources. */
                    const uintptr_t unmap_sorted_begin = reinterpret_cast<uintptr_t>(p_sorted_old) & ~(uintptr_t(PAGESIZE) - 1);
                    const uintptr_t unmap_sorted_end = reinterpret_cast<uintptr_t>(p_sorted) & ~(uintptr_t(PAGESIZE) - 1);
                    const ptrdiff_t unmap_sorted_length = unmap_sorted_end - unmap_sorted_begin;
                    const uintptr_t unmap_out_begin = reinterpret_cast<uintptr_t>(p_out_old) & ~(uintptr_t(PAGESIZE) - 1);
                    const uintptr_t unmap_out_end = reinterpret_cast<uintptr_t>(p_out) & ~(uintptr_t(PAGESIZE) - 1);
                    const ptrdiff_t unmap_out_length = unmap_out_end - unmap_out_begin;

                    if (unmap_sorted_length) {
                        //madvise(reinterpret_cast<void*>(unmap_sorted_begin), unmap_sorted_length, MADV_DONTNEED);
                        if (munmap(reinterpret_cast<void*>(unmap_sorted_begin), unmap_sorted_length))
                            err(EXIT_FAILURE, "Failed to unmap the merged part of the in-memory sorted data");
                    }
                    if (unmap_out_length) {
                        //madvise(reinterpret_cast<void*>(unmap_out_begin), unmap_out_length, MADV_DONTNEED);
                        if (munmap(reinterpret_cast<void*>(unmap_out_begin), unmap_out_length))
                            err(EXIT_FAILURE, "Failed to unmap the solved part of the mmap'd output file");
                    }
                    if (munmap(bucket.addr, bucket.size))
                        err(EXIT_FAILURE, "Failed to unmap the bucket");

                    const auto t_resource_end = ch::high_resolution_clock::now();
                    d_mmap_total += t_resource_end - t_merge_bucket_end;

                    std::cerr << "  Unmap resources took "
                              << ch::duration_cast<ch::nanoseconds>(t_resource_end - t_merge_bucket_end).count() / 1e6
                              << " ms\n";
                } else {
#ifndef NDEBUG
                    std::cerr << "  Empty bucket.  Skip.\n";
#endif
                }
                if (fclose(bucket.file))
                    warn("Failed to close bucket file");
                free(bucket.buffer);
            }
        }
        /* Finish the sorted in-memory data. */
        while (p_sorted != sorted_end)
            *p_out++ = *p_sorted++;

#ifdef WITH_PCM
        the_PCM.getAllCounterStates(pcm_after.system, pcm_after.sockets, pcm_after.cores);
#endif

        const auto t_end = ch::high_resolution_clock::now();

        /* Release resources. */
        if (munmap(in_memory_buffer, num_bytes_to_sort))
            err(EXIT_FAILURE, "Failed to unmap the in-memory buffer");

#ifdef WITH_PCM
        /* Evaluate PCM. */
        const double avg_freq = getActiveAverageFrequency(pcm_before.system, pcm_after.system);
        const double core_IPC = getCoreIPC(pcm_before.system, pcm_after.system);
        const double total_exec_usage = getTotalExecUsage(pcm_before.system, pcm_after.system);
        const double L3_hit_ratio = getL3CacheHitRatio(pcm_before.system, pcm_after.system);
        const double cycles_lost_l3_misses = getCyclesLostDueL3CacheMisses(pcm_before.system, pcm_after.system);
        const auto bytes_read = getBytesReadFromMC(pcm_before.system, pcm_after.system);
        const auto bytes_written = getBytesWrittenToMC(pcm_before.system, pcm_after.system);

        std::cerr << "Performance Counters for the Merge Phase:"
                  << "\n  average core frequency: " << (avg_freq / 1e9) << " GHz"
                  << "\n  average number of retired instructions per core cycle: " << core_IPC
                  << "\n  average number of retired instructions per time intervall: " << total_exec_usage
                  << "\n  L3 cache hit ratio: " << L3_hit_ratio * 100 << "%"
                  << "\n  estimated core cycles lost due to L3 cache misses: " << cycles_lost_l3_misses * 100 << "%"
                  << "\n  bytes read from DRAM memory controllers: " << double(bytes_read) / (1024 * 1024) << " MiB"
                  << "\n  bytes written to DRAM memory controllers: " << double(bytes_written) / (1024 * 1024) << " MiB"
                  << std::endl;
#endif

        /* Report times and throughput. */
        {
            constexpr unsigned long MiB = 1024 * 1024;

            const auto d_read_s = ch::duration_cast<ch::milliseconds>(t_begin_partition - t_begin_read).count() / 1e3;
            const auto d_sort_s = ch::duration_cast<ch::milliseconds>(t_end_sort - t_begin_sort).count() / 1e3;
            const auto d_partition_s = ch::duration_cast<ch::milliseconds>(t_end_partition - t_begin_partition).count() / 1e3;
            const auto d_merge_s = ch::duration_cast<ch::milliseconds>(t_end - t_merge_begin).count() / 1e3;
            const auto d_total_s = ch::duration_cast<ch::milliseconds>(t_end - t_begin_read).count() / 1e3;

            const auto throughput_read_mbs = num_bytes_to_sort / MiB / d_read_s;
            const auto throughput_sort_mbs = num_bytes_to_sort / MiB / d_sort_s;
            const auto throughput_partition_mbs = num_bytes_to_partition / MiB / d_partition_s;
            const auto throughput_merge_mbs = size_in_bytes / MiB / d_merge_s;

            std::cerr << "read:      " << d_read_s << " s (" << throughput_read_mbs << " MiB/s)\n"
                      << "sort:      " << d_sort_s << " s (" << throughput_sort_mbs << " MiB/s)\n"
                      << "partition: " << d_partition_s << " s (" << throughput_partition_mbs << " MiB/s)\n"
                      << "merge:     " << d_merge_s << " s (" << throughput_merge_mbs << " MiB/s)\n"
                      << "total:     " << d_total_s << " s\n";

            std::cerr << "d_mmap_total: " << ch::duration_cast<ch::milliseconds>(d_mmap_total).count() / 1e3 << " s\n"
                      << "d_sort_total: " << ch::duration_cast<ch::milliseconds>(d_sort_total).count() / 1e3 << " s\n"
                      << "d_merge_total: " << ch::duration_cast<ch::milliseconds>(d_merge_total).count() / 1e3 << " s\n";
        }

#ifdef SUBMISSION
        std::this_thread::sleep_for(20s);
#endif
    }

    /* Release resources. */
    close(fd_in);
    close(fd_out);

    std::exit(EXIT_SUCCESS);
}
