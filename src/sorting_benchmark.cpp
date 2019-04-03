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
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>


namespace ch = std::chrono;


constexpr unsigned NUM_THREADS_READ = 16;
constexpr std::size_t NUM_BLOCKS_PER_SLAB = 256;
constexpr unsigned NUM_RUNS = 3;


void sorting_benchmark(record *first, record *last);

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
void partial_read(const thread_info *ti)
{
    auto remaining = ti->count;
    auto offset = ti->offset;
    uint8_t *dst = reinterpret_cast<uint8_t*>(ti->buffer);

    while (ti->slab_size < remaining) {
        const auto read = pread(ti->fd, &dst[offset], ti->slab_size, offset);
        if (read == -1) {
            warn("Failed to read %ld bytes from file '%s' at offset %ld", ti->slab_size, ti->path, offset);
            return;
        }
        remaining -= read;
        offset += read;
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
void partial_write(const thread_info *ti)
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
    if (argc != 2) {
        std::cerr << "USAGE:\n\t" << argv[0] << " <INPUT-FILE>" << std::endl;
        std::exit(EXIT_FAILURE);
    }

    /* Disable synchronization with C stdio. */
    std::ios::sync_with_stdio(false);

    /* Open input file and get file stats. */
    int fd_in = open(argv[1], O_RDONLY);
    if (fd_in == -1)
        err(EXIT_FAILURE, "Could not open file '%s'", argv[1]);
    struct stat stat_in;
    if (fstat(fd_in, &stat_in))
        err(EXIT_FAILURE, "Could not get status of file '%s'", argv[1]);
    const std::size_t num_records = stat_in.st_size / sizeof(record);
    const std::size_t slab_size = NUM_BLOCKS_PER_SLAB * stat_in.st_blksize;
    const std::size_t num_slabs = std::ceil(double(stat_in.st_size) / slab_size);

    /* MMap the output file. */
    void *buffer = mmap(/* addr=   */ nullptr,
                        /* length= */ stat_in.st_size,
                        /* prot=   */ PROT_READ|PROT_WRITE,
                        /* flags=  */ MAP_PRIVATE|MAP_ANONYMOUS,
                        /* fd=     */ -1,
                        /* offset= */ 0);
    if (buffer == MAP_FAILED)
        err(EXIT_FAILURE, "Could not allocate buffer with mmap");

    /* Spawn threads to concurrently read file. */
    {
        std::array<std::thread, NUM_THREADS_READ> threads;
        std::array<thread_info, NUM_THREADS_READ> thread_infos;
        const std::size_t num_slabs_per_thread = num_slabs / NUM_THREADS_READ;
        const std::size_t count_per_thread = num_slabs_per_thread * slab_size;
        for (unsigned tid = 0; tid != NUM_THREADS_READ; ++tid) {
            const std::size_t offset = tid * count_per_thread;
            const std::size_t count = tid == NUM_THREADS_READ - 1 ? stat_in.st_size - offset : count_per_thread;
            thread_infos[tid] = thread_info {
                .tid = tid,
                .fd = fd_in,
                .path = argv[1],
                .offset = tid * count_per_thread,
                .count = count,
                .buffer = buffer,
                .slab_size = slab_size,
            };
            threads[tid] = std::thread(partial_read, &thread_infos[tid]);
        }

        /* Join threads. */
        for (auto &t : threads) {
            if (t.joinable())
                t.join();
        }
    }

    /* Run sorting benchmark. */
    {
        std::cerr << "Run the sorting benchmark." << std::endl;
        record *records = reinterpret_cast<record*>(buffer);
        sorting_benchmark(records, records + num_records);
    }

    /* Release resources. */
    close(fd_in);

    std::exit(EXIT_SUCCESS);
}

void sorting_benchmark(record *first, record *last)
{
    /* TODO */

    /* Output format:
     *
     *      Algorithm, Size, Time
     */

    constexpr double SCALE_FACTOR = 1.2;
    const std::size_t num_records = last - first;
    auto buffer = new record[num_records];

    std::cout << "algorithm, size, time\n";

#define BENCHMARK(ALGO) { \
    /* Iterate over all sizes. */ \
    for (std::size_t size = 100; size < num_records; size *= SCALE_FACTOR) { \
        /* Perform multiple runs for stable results. */ \
        for (std::size_t i = 0; i != NUM_RUNS; ++i) { \
            std::cout << #ALGO << ", " << size << ", "; \
            std::cout.flush(); \
\
            /* Freshly initialize the buffer. */ \
            std::copy(first, first + size, buffer); \
\
            /* Sort the buffer. */ \
            auto t_begin = ch::high_resolution_clock::now(); \
            (ALGO)(buffer, buffer + size); \
            auto t_end = ch::high_resolution_clock::now(); \
\
            /* Validate the sort. */ \
            bool success = std::is_sorted(buffer, buffer + size); \
\
            if (success) \
                std::cout << ch::duration_cast<ch::microseconds>(t_end - t_begin).count(); \
            else \
                std::cout << "NaN"; \
            std::cout << std::endl; \
            if (not success) std::abort(); \
        } \
    } \
}

    BENCHMARK(std::sort);
    BENCHMARK(__gnu_parallel::sort);
    BENCHMARK(american_flag_sort);
    BENCHMARK(american_flag_sort_MT);
    BENCHMARK(my_hybrid_sort);
    BENCHMARK(my_hybrid_sort_MT);

    delete[] buffer;
}
