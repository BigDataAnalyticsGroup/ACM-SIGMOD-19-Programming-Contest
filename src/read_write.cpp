//===== read_write.cpp =================================================================================================
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
//      This is a micro benchmark to measure I/O times.
//
//======================================================================================================================


#include "record.hpp"
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdlib>
#include <err.h>
#include <fcntl.h>
#include <iostream>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>


namespace ch = std::chrono;


constexpr std::size_t IN_MEMORY_THRESHOLD = 28L * 1024 * 1024 * 1024; // 28 GiB
constexpr unsigned NUM_THREADS = 16;
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
    record *records; ///< array of records
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

#if 0
    for (std::size_t i = 0; i != ti->num_records; ++i) {
        if (fread(&ti->records[i], sizeof(record), 1, file) != 1) {
            warn("Failed to read %luth record from file '%s' starting at offset %lu", i, ti->path, ti->offset);
            return;
        }
    }
#else
    if (fread(ti->records, sizeof(record), ti->num_records, file) != ti->num_records) {
        warn("Failed to read %lu records from file '%s' starting at offset %lu", ti->num_records, ti->path, ti->offset);
        return;
    }
#endif

    free(streambuf);
    fclose(file);
}

/** Write a part of the sorted data to the output file. */
void partial_write(const thread_info *ti)
{
    /* Allocate stream buffer. */
    char *streambuf = reinterpret_cast<char*>(malloc(STREAM_BUFFER_SIZE));
    if (not streambuf) {
        warn("Failed to allocate stream buffer");
        return;
    }

    /* Open output file for writing. */
    FILE *file = fopen(ti->path, "wb");
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

#if 1
    for (std::size_t i = 0; i != ti->num_records; ++i) {
        if (fwrite(&ti->records[i], sizeof(record), 1, file) != 1) {
            warn("Failed to read %luth record from file '%s' starting at offset %lu", i, ti->path, ti->offset);
            return;
        }
    }
#else
    if (fwrite(ti->records, sizeof(record), ti->num_records, file) != ti->num_records) {
        warn("Failed to read %luth record from file '%s' starting at offset %lu", ti->num_records, ti->path, ti->offset);
        return;
    }
#endif

    free(streambuf);
    fclose(file);
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

        /* Spawn threads to read file. */
        std::array<std::thread, NUM_THREADS> threads;
        std::array<thread_info, NUM_THREADS> thread_infos;
        const std::size_t num_records_per_thread = num_records / NUM_THREADS;
        for (unsigned tid = 0; tid != NUM_THREADS; ++tid) {
            auto &ti = thread_infos[tid];
            ti.tid = tid;
            ti.path = argv[1];
            ti.offset = tid * num_records_per_thread;
            ti.num_records = tid == NUM_THREADS - 1 ? num_records - ti.offset : num_records_per_thread;
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

        const auto t_begin_write = ch::high_resolution_clock::now();

        /* Write the sorted data to the output file. */
        std::cerr << "Write entire file back to disk.\n";
        for (unsigned tid = 0; tid != NUM_THREADS; ++tid) {
            auto &ti = thread_infos[tid];
            ti.path = argv[2];
            std::cerr << "Thread " << ti.tid << ": Write " << ti.path << " at offset " << ti.offset << " for "
                      << ti.num_records << " records from location " << ti.records << '\n';
            threads[tid] = std::thread(partial_write, &thread_infos[tid]);
        }

        /* Join threads. */
        for (auto &t : threads) {
            if (t.joinable())
                t.join();
        }

        const auto t_finish = ch::high_resolution_clock::now();

        /* Report times and throughput. */
        {
            constexpr unsigned long MiB = 1024 * 1024;

            const auto d_read_s = ch::duration_cast<ch::milliseconds>(t_begin_write - t_begin_read).count() / 1e3;
            const auto d_write_s = ch::duration_cast<ch::milliseconds>(t_finish - t_begin_write).count() / 1e3;

            const auto throughput_read_mbs = size_in_bytes / MiB / d_read_s;
            const auto throughput_write_mbs = size_in_bytes / MiB / d_write_s;

            std::cout << "read: " << d_read_s << " s (" << throughput_read_mbs << " MiB/s)\n"
                      << "write: " << d_write_s << " s (" << throughput_write_mbs << " MiB/s)\n";
        }
    } else {
        /* TODO Not yet implemented */
        std::abort();
    }


    std::exit(EXIT_SUCCESS);
}
