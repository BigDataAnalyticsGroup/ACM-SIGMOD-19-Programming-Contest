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
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>

#include <sstream>


#define CONCURRENT_WRITE 1


namespace ch = std::chrono;


constexpr std::size_t IN_MEMORY_THRESHOLD = 28L * 1024 * 1024 * 1024; // 28 GiB
constexpr unsigned NUM_THREADS_READ = 16;
constexpr unsigned NUM_THREADS_WRITE = 16;
constexpr std::size_t NUM_BLOCKS_PER_SLAB = 256;


uint8_t k0;


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
    if (argc != 3) {
        std::cerr << "USAGE:\n\t" << argv[0] << " <INPUT-FILE> <OUTPUT-FILE>" << std::endl;
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

    std::cerr << "Input file \"" << argv[1] << "\" is " << stat_in.st_size << " bytes large, contains "
              << num_records << " records in blocks of size " << stat_in.st_blksize << " bytes.\n";
    std::cerr << "Perform I/O in slabs of " << NUM_BLOCKS_PER_SLAB << " blocks; " << slab_size
              << " bytes per slab and I/O.  " << num_slabs << " slabs in total.\n";

    /* Open output file and allocate sufficient space. */
    int fd_out = open(argv[2], O_CREAT|O_TRUNC|O_RDWR, 0644);
    if (fd_out == -1)
        err(EXIT_FAILURE, "Could not open file '%s'", argv[2]);
    if (fallocate(fd_out, /* mode = */ 0, /* offset= */ 0, /* len= */ stat_in.st_size)) {
        if (errno == EOPNOTSUPP) {
            if (ftruncate(fd_out, stat_in.st_size))
                err(EXIT_FAILURE, "Could not truncate file '%s' to size %lu", argv[2], stat_in.st_size);
        } else {
            err(EXIT_FAILURE, "Could not allocate space for output file '%s'", argv[2]);
        }
    }

    /* Select whether the workload fits into main memory or requires an external memory algorithm. */
    if (std::size_t(stat_in.st_size) < IN_MEMORY_THRESHOLD) {
        const auto t_begin_read = ch::high_resolution_clock::now();
        std::cerr << "Read entire file into main memory.\n";

        /* MMap the output file. */
        void *buffer = mmap(/* addr=   */ nullptr,
                            /* length= */ stat_in.st_size,
                            /* prot=   */ PROT_READ|PROT_WRITE,
                            /* flags=  */ MAP_SHARED,
                            /* fd=     */ fd_out,
                            /* offset= */ 0);
        if (buffer == MAP_FAILED)
            err(EXIT_FAILURE, "Could not map output file '%s' into memory", argv[2]);

        std::cerr << "Allocate buffer of " << stat_in.st_size << " bytes size at address " << buffer << '\n';

        /* Spawn threads to concurrently read file. */
        {
            std::array<std::thread, NUM_THREADS_READ> threads;
            std::array<thread_info, NUM_THREADS_READ> thread_infos;
            const std::size_t num_slabs_per_thread = num_slabs / NUM_THREADS_READ;
            const std::size_t count_per_thread = num_slabs_per_thread * slab_size;
            for (unsigned tid = 0; tid != NUM_THREADS_READ; ++tid) {
                const std::size_t offset = tid * count_per_thread;
                const std::size_t count = tid == NUM_THREADS_READ - 1 ? stat_in.st_size - offset : count_per_thread;
                std::cerr << "Thread " << tid << " reads \"" << argv[1] << "\" at offset " << offset << " for "
                          << count << " bytes.\n";
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

        const auto t_begin_sort = ch::high_resolution_clock::now();

        /* Sort the records. */
        {
            record *records = reinterpret_cast<record*>(buffer);
            std::cerr << "Sort the data.\n";
            std::sort(records, records + num_records);
            assert(std::is_sorted(records, records + num_records));
        }

        const auto t_begin_write = ch::high_resolution_clock::now();

        /* Write the sorted data to the output file. */
        std::cerr << "Write sorted data back to disk.\n";
        msync(buffer, stat_in.st_size, MS_ASYNC);
        munmap(buffer, stat_in.st_size);

        const auto t_finish = ch::high_resolution_clock::now();

        /* Report times and throughput. */
        {
            constexpr unsigned long MiB = 1024 * 1024;

            const auto d_read_s = ch::duration_cast<ch::milliseconds>(t_begin_sort - t_begin_read).count() / 1e3;
            const auto d_sort_s = ch::duration_cast<ch::milliseconds>(t_begin_write - t_begin_sort).count() / 1e3;
            const auto d_write_s = ch::duration_cast<ch::milliseconds>(t_finish - t_begin_write).count() / 1e3;

            const auto throughput_read_mbs = stat_in.st_size / MiB / d_read_s;
            const auto throughput_sort_mbs = stat_in.st_size / MiB / d_sort_s;
            const auto throughput_write_mbs = stat_in.st_size / MiB / d_write_s;

            std::cerr << "read: " << d_read_s << " s (" << throughput_read_mbs << " MiB/s)\n"
                      << "sort: " << d_sort_s << " s (" << throughput_sort_mbs << " MiB/s)\n"
                      << "write: " << d_write_s << " s (" << throughput_write_mbs << " MiB/s)\n";
        }
    } else {
        /* TODO Not yet implemented */
        std::abort();
    }

    /* Release resources. */
    close(fd_in);
    close(fd_out);

    std::exit(EXIT_SUCCESS);
}
