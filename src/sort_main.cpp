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

constexpr std::size_t FILE_SIZE_SMALL   = 00UL * 1000 * 1000 * 1000; // 10 GB
constexpr std::size_t FILE_SIZE_MEDIUM  = 20UL * 1000 * 1000 * 1000; // 20 GB
//constexpr std::size_t FILE_SIZE_MEDIUM  = 00UL * 1000 * 1000 * 1000; // 20 GB
constexpr std::size_t FILE_SIZE_LARGE   = 60UL * 1000 * 1000 * 1000; // 60 GB

constexpr std::size_t IN_MEMORY_BUFFER_SIZE = 26UL * 1024 * 1024 * 1024; // 26 GiB
//constexpr std::size_t IN_MEMORY_BUFFER_SIZE = 3UL * 1024 * 1024 * 1024; // 26 GiB
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
    /* By evaluation, we figured that logical cores 0-9,20-29 belong to NUMA region 0.  Explicitly bind this process to
     * these logical cores to avoid NUMA.  */
    {
        cpu_set_t *cpus = CPU_ALLOC(40);
        if (not cpus)
            err(EXIT_FAILURE, "Failed to allocate CPU_SET of 40 CPUs");
        const auto size = CPU_ALLOC_SIZE(40);
        CPU_ZERO_S(size, cpus);
        for (int cpu = 0; cpu != 10; ++cpu)
            CPU_SET_S(cpu, size, cpus);
        for (int cpu = 20; cpu != 30; ++cpu)
            CPU_SET_S(cpu, size, cpus);
        assert(CPU_COUNT_S(size, cpus) == 20 and "allocated incorrect number of logical CPUs");

        /* Bind process and all children to the desired logical CPUs. */
        sched_setaffinity(0 /* this thread */, size, cpus);

        CPU_FREE(cpus);
    }
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
            //my_hybrid_sort_MT(records, records + num_records, thread_pool);
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

        const auto t_begin_partition = ch::high_resolution_clock::now();

        /* Create the output files for the buckets. */
        FILE *bucket_files[NUM_BUCKETS];
        void *file_buffers[NUM_BUCKETS];
        {
            std::ostringstream path;
            for (std::size_t bucket_id = 0; bucket_id != NUM_BUCKETS; ++bucket_id) {
                path.str("");
                path << OUTPUT_PATH << "/bucket_" << std::setfill('0') << std::setw(3) << bucket_id << ".bin";
                FILE *file = fopen(path.str().c_str(), "w");
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

        const std::size_t num_records_to_partition = size_in_bytes <= IN_MEMORY_BUFFER_SIZE ? 0 :
            (size_in_bytes - IN_MEMORY_BUFFER_SIZE) / sizeof(record);
        const std::size_t num_bytes_to_partition = num_records_to_partition * sizeof(record);

        /* Read first part and partition. */
        if (num_records_to_partition) {
            std::cerr << "Read and partition the first " << num_records_to_partition << " records ("
                      << num_bytes_to_partition << " bytes).\n";

            auto partition = [fd_in, argv, bucket_files](std::size_t offset, std::size_t count) {
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
                        auto dst_file = bucket_files[r.key[0]];
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
                        auto dst_file = bucket_files[r.key[0]];
                        fwrite(&r, sizeof(r), 1, dst_file);
                    }
                }
            };

            std::array<std::thread, NUM_THREADS_PARTITION> threads;
            const std::size_t count_per_thread = num_records_to_partition / NUM_THREADS_PARTITION;
            for (unsigned tid = 0; tid != NUM_THREADS_PARTITION; ++tid) {
                const std::size_t offset = tid * count_per_thread;
                const std::size_t count = tid == NUM_THREADS_PARTITION - 1 ? num_records_to_partition - offset : count_per_thread;
                threads[tid] = std::thread(partition, offset * sizeof(record), count);
            }
            for (unsigned tid = 0; tid != NUM_THREADS_PARTITION; ++tid)
                threads[tid].join();
        }

        const auto t_begin_read = ch::high_resolution_clock::now();

        /* Concurrently read the remaining portion of the file. */
        std::cerr << "Read the remaining " << (num_records - num_records_to_partition) << " records ("
                  << (size_in_bytes - num_bytes_to_partition) << " bytes).\n";
        void *tmp = mmap(nullptr, IN_MEMORY_BUFFER_SIZE, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS,
                         /* fd= */ -1, 0);
        if (tmp == MAP_FAILED)
            err(EXIT_FAILURE, "Failed to map temporary read buffer");
        read_concurrent(fd_in, tmp, IN_MEMORY_BUFFER_SIZE, num_records_to_partition * sizeof(record));

        const auto t_begin_sort = ch::high_resolution_clock::now();

        std::cerr << "TODO: Sort the data.\n";

        /* Report times and throughput. */
        {
            constexpr unsigned long MiB = 1024 * 1024;

            const auto d_partition_s = ch::duration_cast<ch::milliseconds>(t_begin_read - t_begin_partition).count() / 1e3;
            const auto d_read_s = ch::duration_cast<ch::milliseconds>(t_begin_sort - t_begin_read).count() / 1e3;
            //const auto d_sort_s = ch::duration_cast<ch::milliseconds>(t_begin_write - t_begin_sort).count() / 1e3;
            //const auto d_write_s = ch::duration_cast<ch::milliseconds>(t_finish - t_begin_write).count() / 1e3;

            const auto throughput_partition_mbs = num_records_to_partition * sizeof(record) / MiB / d_partition_s;
            const auto throughput_read_mbs = (size_in_bytes - num_records_to_partition * sizeof(record)) / MiB / d_read_s;
            //const auto throughput_sort_mbs = size_in_bytes / MiB / d_sort_s;
            //const auto throughput_write_mbs = size_in_bytes / MiB / d_write_s;

            std::cerr << "partition: " << d_partition_s << " s (" << throughput_partition_mbs << " MiB/s)\n"
                      << "read:      " << d_read_s << " s (" << throughput_read_mbs << " MiB/s)\n";
                      //<< "sort:      " << d_sort_s << " s (" << throughput_sort_mbs << " MiB/s)\n"
                      //<< "write:     " << d_write_s << " s (" << throughput_write_mbs << " MiB/s)\n";
        }

        for (std::size_t bucket_id = 0; bucket_id != NUM_BUCKETS; ++bucket_id) {
            if (fclose(bucket_files[bucket_id]))
                warn("Failed to close output file");
            free(file_buffers[bucket_id]);
        }

        munmap(tmp, IN_MEMORY_BUFFER_SIZE);
        std::cerr << "Not yet supported.\n";
        std::exit(EXIT_FAILURE);
    }

    /* Release resources. */
    close(fd_in);
    close(fd_out);

    std::exit(EXIT_SUCCESS);
}
