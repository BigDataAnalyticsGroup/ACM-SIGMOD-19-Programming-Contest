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
constexpr std::size_t FILE_SIZE_LARGE   = 60UL * 1000 * 1000 * 1000; // 60 GB

constexpr unsigned NUM_THREADS_READ = 4;
constexpr std::size_t NUM_BLOCKS_PER_SLAB = 256;


/** Information for the threads. */
struct thread_info
{
    unsigned tid; ///< id of this thread
    int fd; ///< file descriptor of the file
    const char *path; ///< the path to the file to read from / write to
    std::size_t offset; ///< offset from start of the file in bytes
    std::size_t count; ///< number of bytes to read from / write to the file
    void *buffer; ///< I/O buffer to read into / write out
    std::size_t slab_size; ///< the size of a slab, the granule at which I/O is performed; a multiple of the preferred block size
};

/** Load a part of a file into a destination memory location. */
void read_in_slabs(thread_info *ti)
{
    auto remaining = ti->count;
    auto offset = ti->offset;
    uint8_t *dst = reinterpret_cast<uint8_t*>(ti->buffer);

    while (ti->slab_size <= remaining) {
        const auto read = pread(ti->fd, &dst[offset], ti->slab_size, offset);
        if (read == -1) {
            warn("Failed to read %ld bytes from file '%s' at offset %ld", ti->slab_size, ti->path, offset);
            return;
        }
        remaining -= read;
        offset += read;
        /* TODO what if pread() did not read an entire slab?  Should we fall back to regular reading or should we try to
         * recover by reading up to the end of the slab? */
    }

    while (remaining) {
        const auto read = pread(ti->fd, &dst[offset], remaining, offset);
        if (read == -1) {
            warn("Failed to read %ld bytes from file '%s' at offset %ld", remaining, ti->path, offset);
            return;
        }
        remaining -= read;
        offset += read;
    }
}

/** Write a part of the sorted data to the output file. */
void write_in_slabs(thread_info *ti)
{
    auto remaining = ti->count;
    auto offset = ti->offset;
    uint8_t *src = reinterpret_cast<uint8_t*>(ti->buffer);

    while (ti->slab_size < remaining) {
        const auto written = pwrite(ti->fd, &src[offset], ti->slab_size, offset);
        if (written == -1) {
            warn("Failed to write %ld bytes from buffer at offset %ld to file '%s'", ti->slab_size, offset, ti->path);
            return;
        }
        remaining -= written;
        offset += written;
        /* TODO what if pwrite() did not write an entire slab?  Should we fall back to regular writing or should we try
         * to recover by writing out to the end of the slab? */
    }

    while (remaining) {
        const auto written = pwrite(ti->fd, &src[offset], remaining, offset);
        if (written == -1) {
            warn("Failed to write %ld bytes from buffer at offset %ld to file '%s'", remaining, offset, ti->path);
            return;
        }
        remaining -= written;
        offset += written;
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

    /* Determine number of threads to use.  Corresponds to logical cores on one NUMA region. */
#ifdef SUBMISSION
    const auto num_threads = 20;
#else
    const auto num_threads = std::thread::hardware_concurrency();
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
    const std::size_t slab_size = NUM_BLOCKS_PER_SLAB * stat_in.st_blksize;
    const std::size_t num_slabs = std::ceil(double(size_in_bytes) / slab_size);
    std::cerr << "Input file \"" << argv[1] << "\" is " << size_in_bytes << " bytes large, contains "
              << num_records << " records in blocks of size " << stat_in.st_blksize << " bytes.\n";
    //std::cerr << "Perform I/O in slabs of " << NUM_BLOCKS_PER_SLAB << " blocks; " << slab_size
              //<< " bytes per slab and I/O.  " << num_slabs << " slabs in total.\n";

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
        {
#if READ_CONCURRENT
            std::cerr << "Concurrently read entire file into the output buffer.\n";
            std::array<thread_info, NUM_THREADS_READ> thread_infos;
            std::array<std::thread, NUM_THREADS_READ> threads;
            const std::size_t num_slabs_per_thread = num_slabs / NUM_THREADS_READ;
            const std::size_t count_per_thread = num_slabs_per_thread * slab_size;
            for (unsigned tid = 0; tid != NUM_THREADS_READ; ++tid) {
                const std::size_t offset = tid * count_per_thread;
                const std::size_t count = tid == NUM_THREADS_READ - 1 ? size_in_bytes - offset : count_per_thread;
                thread_infos[tid] = thread_info {
                    .tid = tid,
                    .fd = fd_in,
                    .path = argv[1],
                    .offset = tid * count_per_thread,
                    .count = count,
                    .buffer = output,
                    .slab_size = slab_size,
                };
                threads[tid] = std::thread(read_in_slabs, &thread_infos[tid]);
            }

            /* Join threads. */
            for (unsigned tid = 0; tid != NUM_THREADS_READ; ++tid)
                threads[tid].join();
#else
            std::cerr << "Sequentially read entire file into the output buffer.\n";
            char *buf = reinterpret_cast<char*>(output);
            auto remaining = size_in_bytes;
            while (slab_size <= remaining) {
                const auto num_read = read(fd_in, buf, slab_size);
                if (num_read == -1)
                    err(EXIT_FAILURE, "Failed to read %lu bytes from file '%s'", slab_size, argv[1]);
                remaining -= num_read;
                buf += num_read;
            }
            while (remaining) {
                const auto num_read = read(fd_in, buf, remaining);
                if (num_read == -1)
                    err(EXIT_FAILURE, "Failed to read %lu bytes from file '%s'", remaining, argv[1]);
                remaining -= num_read;
                buf += num_read;
            }
#endif
        }

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
        std::cerr << "Not yet supported.\n";
        std::exit(EXIT_FAILURE);
    }

    /* Release resources. */
    close(fd_in);
    close(fd_out);

    std::exit(EXIT_SUCCESS);
}
