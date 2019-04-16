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
#include <parallel/algorithm>
#include <sched.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>


#define READ_CONCURRENT 1


namespace ch = std::chrono;
using namespace std::chrono_literals;

constexpr std::size_t FILE_SIZE_SMALL   = 00UL * 1000 * 1000 * 1000; // 10 GB
//constexpr std::size_t FILE_SIZE_MEDIUM  = 20UL * 1000 * 1000 * 1000; // 20 GB
constexpr std::size_t FILE_SIZE_MEDIUM  = 00UL * 1000 * 1000 * 1000; // 20 GB
constexpr std::size_t FILE_SIZE_LARGE   = 60UL * 1000 * 1000 * 1000; // 60 GB

//constexpr std::size_t IN_MEMORY_BUFFER_SIZE = 26UL * 1024 * 1024 * 1024; // 26 GiB
constexpr std::size_t IN_MEMORY_BUFFER_SIZE = 1UL * 1024 * 1024 * 1024; // 26 GiB
constexpr std::size_t NUM_BLOCKS_PER_SLAB = 1024;

#ifdef SUBMISSION
constexpr unsigned NUM_THREADS_READ = 20;
constexpr unsigned NUM_THREADS_PARTITION = 10;
constexpr const char * const OUTPUT_PATH = "/output-disk";
#else
constexpr unsigned NUM_THREADS_READ = 4;
constexpr unsigned NUM_THREADS_PARTITION = 16;
constexpr const char * const OUTPUT_PATH = "./buckets";
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

        std::this_thread::sleep_for(10s);
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
        std::array<FILE*, NUM_BUCKETS> bucket_files;
        std::array<void*, NUM_BUCKETS> file_buffers;
        std::array<std::atomic_uint_fast32_t, NUM_BUCKETS> bucket_size{ {0} };
        {
            std::ostringstream path;
            for (std::size_t bucket_id = 0; bucket_id != NUM_BUCKETS; ++bucket_id) {
                path.str("");
                path << OUTPUT_PATH << "/bucket_" << std::setfill('0') << std::setw(3) << bucket_id << ".bin";
                FILE *file = fopen(path.str().c_str(), "w+b");
                if (not file)
                    err(EXIT_FAILURE, "Could not open bucket file '%s'", path.str().c_str());

                /* Assign custom, large file buffer. */
                const std::size_t buffer_size = 256 * stat_in.st_blksize;
                void *buffer = malloc(buffer_size);
                if (not buffer)
                    err(EXIT_FAILURE, "Failed to allocate I/O buffer");

                if (setvbuf(file, static_cast<char*>(buffer), _IOFBF, buffer_size))
                    err(EXIT_FAILURE, "Failed to set custom buffer for file");

                file_buffers[bucket_id] = buffer;
                bucket_files[bucket_id] = file;
            }
        }

        /* Read second part and partition. */
        if (num_records_to_partition) {
            std::cerr << "Read and partition the remaining " << num_records_to_partition << " records ("
                      << num_bytes_to_partition << " bytes), starting at offset " << num_bytes_to_sort << ".\n";

            auto partition = [fd_in, argv, &bucket_files, &bucket_size](std::size_t offset, std::size_t count) {
                constexpr std::size_t RECORDS_PER_BUFFER = 10000;
                record buf[RECORDS_PER_BUFFER];

                /* Read as many records at once as fit into our buffer. */
                while (count >= RECORDS_PER_BUFFER) {
                    const auto bytes_read = pread(fd_in, buf, sizeof(buf), offset);
                    if (bytes_read != sizeof(buf)) {
                        warn("Failed to read next records from file '%s' at offset %ld", argv[1], offset);
                        return;
                    }
                    offset += bytes_read;
                    count -= RECORDS_PER_BUFFER;

                    for (auto &r : buf) {
                        const unsigned dst_bucket = r.key[0];
                        bucket_size[dst_bucket].fetch_add(1);
                        auto dst_file = bucket_files[dst_bucket];
                        fwrite(&r, sizeof(r), 1, dst_file);
                    }
                }
                assert(count < RECORDS_PER_BUFFER);

                /* Read remaining records at the end. */
                if (count) {
                    const auto bytes_read = pread(fd_in, buf, count * sizeof(*buf), offset);
                    if (bytes_read != int(count * sizeof(*buf))) {
                        warn("Failed to read next records from file '%s' at offset %ld", argv[1], offset);
                        return;
                    }

                    for (std::size_t i = 0; i != count; ++i) {
                        auto &r = buf[i];
                        const unsigned dst_bucket = r.key[0];
                        bucket_size[dst_bucket].fetch_add(1);
                        auto dst_file = bucket_files[dst_bucket];
                        fwrite(&r, sizeof(r), 1, dst_file);
                    }
                }
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

        thread_sort.join();

#if 0
        /* Find the largest bucket. */
        {
            std::size_t max_bucket = 0;
            unsigned size = bucket_size[max_bucket];
            for (std::size_t bucket_id = 1; bucket_id != NUM_BUCKETS; ++bucket_id) {
                if (bucket_size[bucket_id] > size) {
                    max_bucket = bucket_id;
                    size = bucket_size[bucket_id];
                }
            }
            std::cerr << "The largest bucket is bucket " << max_bucket << " with " << size << " records.\n";
        }
#endif

        const auto t_merge_begin = ch::high_resolution_clock::now();

        /* For each partition, read it, sort it, merge with sorted records, and write out to output file. */
        std::cerr << "Merge the sorted " << num_records_to_sort << " records in memory with the buckets.\n";
        auto p_out = reinterpret_cast<record*>(output);
        auto p_sorted = reinterpret_cast<record*>(in_memory_buffer);
        auto end_sorted = p_sorted + num_records_to_sort;
        assert(std::is_sorted(p_sorted, end_sorted) and "in-memory data is not sorted");
        for (std::size_t bucket_id = 0; bucket_id != NUM_BUCKETS; ++bucket_id) {
            FILE *file = bucket_files[bucket_id];
            if (fflush(file))
                err(EXIT_FAILURE, "Failed to flush bucket %lu", bucket_id);
            int fd = fileno(file);
            struct stat status;
            if (fstat(fd, &status))
                err(EXIT_FAILURE, "Failed to get status of bucket %lu file", bucket_id);

            /* Process non-empty bucket. */
            if (status.st_size != 0) {
                void *mem_bucket = mmap(nullptr, status.st_size, PROT_READ|PROT_WRITE, MAP_PRIVATE, fd, 0);
                if (mem_bucket == MAP_FAILED)
                    err(EXIT_FAILURE, "Failed to mmap bucket %lu file", bucket_id);
                if (madvise(mem_bucket, status.st_size, MADV_SEQUENTIAL))
                    warn("Failed to advise sequential access to the bucket file");
                record *bucket = reinterpret_cast<record*>(mem_bucket);
                const std::size_t num_records_in_bucket = status.st_size / sizeof(record);

                //std::cerr << "Mapping and sorting bucket " << bucket_id << " with " << num_records_in_bucket << " records.\n";
                american_flag_sort_parallel(bucket, bucket + num_records_in_bucket, 0);
                assert(std::is_sorted(bucket, bucket + num_records_in_bucket));

                /* TODO */
                auto p_bucket = bucket;
                auto end_bucket = bucket + num_records_in_bucket;
                const auto p_sorted_old = p_sorted;

#ifndef NDEBUG
                std::cerr << "Merging bucket " << bucket_id << " of " << num_records_in_bucket
                          << " records with sorted data at offset " << (p_sorted - (record*) in_memory_buffer)
                          << " (" << (p_sorted - (record*) in_memory_buffer) * sizeof(record) << " bytes).\n";
#endif

                /* Merge this bucket with the sorted data. */
                while (p_bucket != end_bucket and p_sorted != end_sorted) {
                    assert(p_bucket <= end_bucket);
                    assert(p_sorted <= end_sorted);
                    assert(p_out < reinterpret_cast<record*>(output) + num_records);
                    if (*p_bucket < *p_sorted)
                        *p_out++ = *p_bucket++;
                    else
                        *p_out++ = *p_sorted++;
                }
                assert ((p_bucket == end_bucket) != (p_sorted == end_sorted) and
                        "either the bucket is finished and sorted in-memory data remains or vice versa");
                /* Finish the bucket, if necessary. */
                while (p_bucket != end_bucket)
                    *p_out++ = *p_bucket++;

                /* Release resources. */
                if (1) {
                    /* Compute the address of the page of p_sorted_old */
                    uintptr_t unmap_begin = reinterpret_cast<uintptr_t>(p_sorted_old) & ~(uintptr_t(PAGESIZE) - 1);
                    uintptr_t unmap_end = reinterpret_cast<uintptr_t>(p_sorted) & ~(uintptr_t(PAGESIZE) - 1);
                    if (munmap(reinterpret_cast<void*>(unmap_begin), unmap_end - unmap_begin))
                        err(EXIT_FAILURE, "Failed to unmap the merged part of the in-memory sorted data");
                }
                if (munmap(mem_bucket, status.st_size))
                    err(EXIT_FAILURE, "Failed to unmap the bucket");
            }

            if (fclose(file))
                warn("Failed to close bucket file");
            free(file_buffers[bucket_id]);
        }
        /* Finish the sorted in-memory data. */
        while (p_sorted != end_sorted)
            *p_out++ = *p_sorted++;

        const auto t_end = ch::high_resolution_clock::now();

        /* Report times and throughput. */
        {
            constexpr unsigned long MiB = 1024 * 1024;

            const auto d_read_s = ch::duration_cast<ch::milliseconds>(t_begin_partition - t_begin_read).count() / 1e3;
            const auto d_sort_s = ch::duration_cast<ch::milliseconds>(t_end_sort - t_begin_sort).count() / 1e3;
            const auto d_partition_s = ch::duration_cast<ch::milliseconds>(t_merge_begin - t_begin_partition).count() / 1e3;
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
        }

        std::this_thread::sleep_for(40s);

        /* Release resources. */
        if (munmap(in_memory_buffer, num_bytes_to_sort))
            err(EXIT_FAILURE, "Failed to unmap the in-memory buffer");
        std::cerr << "Not yet supported.\n";
        std::exit(EXIT_FAILURE);
    }

    /* Release resources. */
    close(fd_in);
    close(fd_out);

    std::exit(EXIT_SUCCESS);
}
