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
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdlib>
#include <err.h>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>


#define CONCURRENT_WRITE 1


namespace ch = std::chrono;


constexpr std::size_t IN_MEMORY_THRESHOLD = 28L * 1024 * 1024 * 1024; // 28 GiB
constexpr unsigned NUM_THREADS_READ = 16;
constexpr unsigned NUM_THREADS_WRITE = 16;
constexpr std::size_t SECTOR_SIZE = 512; // byte
constexpr std::size_t STREAM_BUFFER_SIZE = 1024 * SECTOR_SIZE;


uint8_t k0;


/** Information for the threads. */
struct thread_info
{
    unsigned tid; ///< id of this thread
    const char *path; ///< the path to the file to read from / write to
    std::size_t offset; ///< offset in records from start of the file
    std::size_t num_records; ///< number of records to read from / wrtie to file
    record *records; ///< points to the location of the records
    int fd_out; ///< file descriptor of the output file
};

/** Load a part of a file into a destination memory location. */
void partial_read(const thread_info *ti)
{
    /* Allocate stream buffer. */
    char *streambuf = reinterpret_cast<char*>(malloc(STREAM_BUFFER_SIZE));
    if (not streambuf) {
        warn("Failed to allocate stream buffer");
        return;
    }

    /* Open input file for reading. */
    FILE *file = fopen(ti->path, "rb");
    if (not file) {
        warn("Could not open file '%s'", ti->path);
        return;
    }

    /* Switch to custom stream buffer. */
    if (setvbuf(file, streambuf, _IOFBF, STREAM_BUFFER_SIZE))
        warn("Failed to set custom stream buffer");

    /* Seek to offset. */
    if (fseek(file, ti->offset * sizeof(record), SEEK_SET)) {
        warn("Failed to seek to offset %lu in file '%s'", ti->offset, ti->path);
        return;
    }

    if (fread(ti->records, sizeof(record), ti->num_records, file) != ti->num_records) {
        warn("Failed to read %lu records from file '%s' starting at offset %lu", ti->num_records, ti->path, ti->offset);
        return;
    }

    free(streambuf);
    fclose(file);
}

/** Write a part of the sorted data to the output file. */
void partial_write(const thread_info *ti)
{
    /* Verify data is sorted. */
    assert(std::is_sorted(ti->records, ti->records + ti->num_records));

    /* Write data to file. */
    char *data = reinterpret_cast<char*>(ti->records);
    std::size_t remaining = ti->num_records * sizeof(record);
    off_t offset = 0;
    while (remaining) {
        const auto written = pwrite(ti->fd_out, data + offset, remaining, offset);
        if (written == -1) {
            warn("Failed to write %lu records to file '%s' starting at offset %lu", ti->num_records, ti->path, ti->offset);
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

    /* Get input file size. */
    int fd_in = open(argv[1], O_RDONLY);
    if (fd_in == -1)
        err(EXIT_FAILURE, "Could not open file '%s'", argv[1]);
    struct stat statbuf;
    if (fstat(fd_in, &statbuf))
        err(EXIT_FAILURE, "Could not get status of file '%s'", argv[1]);
    close(fd_in);
    const std::size_t size_in_bytes = statbuf.st_size;
    const std::size_t num_records = size_in_bytes / sizeof(record);

    if (size_in_bytes < IN_MEMORY_THRESHOLD) {
        const auto t_begin_read = ch::high_resolution_clock::now();
        std::cerr << "Read entire file into main memory.\n";

        /* Allocate memory for the file. */
        record *records = reinterpret_cast<record*>(malloc(num_records * sizeof(record)));

        /* Spawn threads to cooncurrently read file. */
        {
            std::array<std::thread, NUM_THREADS_READ> threads;
            std::array<thread_info, NUM_THREADS_READ> thread_infos;
            const std::size_t num_records_per_thread = num_records / NUM_THREADS_READ;
            for (unsigned tid = 0; tid != NUM_THREADS_READ; ++tid) {
                auto &ti = thread_infos[tid];
                ti.tid = tid;
                ti.path = argv[1];
                ti.offset = tid * num_records_per_thread;
                ti.num_records = tid == NUM_THREADS_READ - 1 ? num_records - ti.offset : num_records_per_thread;
                ti.records = records + ti.offset;
                std::cerr << "Thread " << ti.tid << ": Read " << ti.path << " at offset " << ti.offset << " for "
                    << ti.num_records << " records to location " << ti.records << '\n';
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
        std::cerr << "Sort the data.\n";
        std::sort(records, records + num_records);
        assert(std::is_sorted(records, records + num_records));

        const auto t_begin_write = ch::high_resolution_clock::now();

        /* Write the sorted data to the output file. */
        std::cerr << "Write sorted data back to disk.\n";
#if CONCURRENT_WRITE
        /* Open output file. */
        int fd_out = open(argv[2], O_CREAT|O_TRUNC|O_WRONLY, 0644);
        if (fd_out == -1)
            err(EXIT_FAILURE, "Could not open file '%s'", argv[2]);
        if (ftruncate(fd_out, size_in_bytes))
            err(EXIT_FAILURE, "Could not truncate file '%s' to size '%lu'", argv[2], size_in_bytes);

        /* Spawn threads to concurrently write file. */
        {
            std::array<std::thread, NUM_THREADS_WRITE> threads;
            std::array<thread_info, NUM_THREADS_WRITE> thread_infos;
            const std::size_t num_records_per_thread = num_records / NUM_THREADS_WRITE;
            for (unsigned tid = 0; tid != NUM_THREADS_WRITE; ++tid) {
                auto &ti = thread_infos[tid];
                ti.tid = tid;
                ti.path = argv[2];
                ti.offset = tid * num_records_per_thread;
                ti.num_records = tid == NUM_THREADS_WRITE - 1 ? num_records - ti.offset : num_records_per_thread;
                ti.records = records + ti.offset;
                ti.fd_out = fd_out;
                std::cerr << "Thread " << ti.tid << ": Write " << ti.path << " at offset " << ti.offset << " for "
                    << ti.num_records << " records from location " << ti.records << '\n';
                threads[tid] = std::thread(partial_write, &thread_infos[tid]);
            }

            /* Join threads. */
            for (auto &t : threads) {
                if (t.joinable())
                    t.join();
            }
        }

        /* Release resources. */
        fsync(fd_out);
        close(fd_out);
#else
        /* Open output file. */
        FILE *out = fopen(argv[2], "wb");
        if (not out)
            err(EXIT_FAILURE, "Could not open file '%s'", argv[2]);
        char *streambuf = reinterpret_cast<char*>(malloc(STREAM_BUFFER_SIZE));
        if (setvbuf(out, streambuf, _IOFBF, STREAM_BUFFER_SIZE))
            warn("Failed to set custom stream buffer");

        if (fwrite(records, 1, size_in_bytes, out) != size_in_bytes)
            warn("Failed to write to output file '%s'", argv[2]);

        /* Release resources. */
        fflush(out);
        free(streambuf);
        fclose(out);
#endif

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
    } else {
        /* TODO Not yet implemented */
        std::abort();
    }

    std::exit(EXIT_SUCCESS);
}
