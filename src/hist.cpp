//===== hist.cpp =======================================================================================================
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
//      This file provides algorithms for histrogram generation.
//
//======================================================================================================================

#include "hist.hpp"

#include "mmap.hpp"
#include "record.hpp"
#include <cassert>
#include <err.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>


histogram_t hist_direct(const char *infile)
{
    /* Open the input file. */
    int fildes = open(infile, O_RDONLY);
    if (-1 == fildes)
        err(EXIT_FAILURE, "Could not open file '%s'", infile);

    /* Retrieve file size in bytes. */
    struct stat stat_in;
    if (fstat(fildes, &stat_in))
        err(EXIT_FAILURE, "Could not stat file '%s'", infile);
    const auto size_in_bytes = stat_in.st_size;

    /* Compute the number of records. */
    const unsigned num_records = size_in_bytes / sizeof(record);

    /* Compute the histogram. */
    char buffer[sizeof(record)];
    std::array<unsigned, 1024> histogram{ 0 };
    for (unsigned i = 0; i != num_records; ++i) {
        read(fildes, buffer, sizeof buffer);
        const uint32_t pid = (uint32_t(buffer[0]) << 2) | (uint32_t(buffer[1]) >> 6);
        assert(pid < 1024);
        ++histogram[pid];
    }

    close(fildes);
    return histogram;
}

histogram_t hist_file(const char *infile)
{
    /* Open the input file. */
    FILE *in = fopen(infile, "r");
    if (not in)
        err(EXIT_FAILURE, "Could not open file '%s'", infile);

    /* Retrieve file size in bytes. */
    struct stat stat_in;
    if (fstat(fileno(in), &stat_in))
        err(EXIT_FAILURE, "Could not stat file '%s'", infile);
    const auto size_in_bytes = stat_in.st_size;

    /* Compute the number of records. */
    const unsigned num_records = size_in_bytes / sizeof(record);

    /* Compute the histogram. */
    std::array<unsigned, 1024> histogram{ 0 };
    for (unsigned i = 0; i != num_records; ++i) {
        const uint32_t k0 = getc_unlocked(in);
        const uint32_t k1 = getc_unlocked(in);
        const uint32_t pid = (k0 << 2) | (k1 >> 6);
        for (unsigned i = 2; i != sizeof(record); ++i)
            getc_unlocked(in);
        assert(pid < 1024);
        ++histogram[pid];
    }
    assert(getc_unlocked(in) == EOF and "expected end-of-file");

    fclose(in);
    return histogram;
}

histogram_t hist_file_seek(const char *infile)
{
    /* Open the input file. */
    FILE *in = fopen(infile, "r");
    if (not in)
        err(EXIT_FAILURE, "Could not open file '%s'", infile);

    /* Retrieve file size in bytes. */
    struct stat stat_in;
    if (fstat(fileno(in), &stat_in))
        err(EXIT_FAILURE, "Could not stat file '%s'", infile);
    const auto size_in_bytes = stat_in.st_size;

    /* Compute the number of records. */
    const unsigned num_records = size_in_bytes / sizeof(record);

    /* Compute the histogram. */
    std::array<unsigned, 1024> histogram{ 0 };
    for (unsigned i = 0; i != num_records; ++i) {
        const uint32_t k0 = getc_unlocked(in);
        const uint32_t k1 = getc_unlocked(in);
        assert(k0 < 256);
        assert(k1 < 256);
        const uint32_t pid = (k0 << 2) | (k1 >> 6);
        assert(pid < 1024);
        ++histogram[pid];
        fseek(in, 98, SEEK_CUR);
    }
    assert(getc_unlocked(in) == EOF and "expected end-of-file");

    fclose(in);
    return histogram;
}

histogram_t hist_file_custom_buffer(const char *infile)
{
    /* Open the input file. */
    FILE *in = std::fopen(infile, "r");
    if (not in)
        err(EXIT_FAILURE, "Could not open file '%s'", infile);

    /* Retrieve file size in bytes. */
    struct stat stat_in;
    if (fstat(fileno(in), &stat_in))
        err(EXIT_FAILURE, "Could not stat file '%s'", infile);
    const auto size_in_bytes = stat_in.st_size;

    /* Compute the number of records. */
    const unsigned num_records = size_in_bytes / sizeof(record);

    /* Use custom buffer. */
    constexpr std::size_t BUFFER_SIZE = 32 * 1024; // 32 KiB
    char *buf = static_cast<char*>(std::aligned_alloc(4096, BUFFER_SIZE));
    setvbuf(in, buf, /* mode = */ _IOFBF, BUFFER_SIZE);

    /* Compute the histogram. */
    std::array<unsigned, 1024> histogram{ 0 };
    for (unsigned i = 0; i != num_records; ++i) {
        const uint32_t k0 = getc_unlocked(in);
        const uint32_t k1 = getc_unlocked(in);
        const uint32_t pid = (k0 << 2) | (k1 >> 6);
        for (unsigned i = 2; i != sizeof(record); ++i)
            getc_unlocked(in);
        assert(pid < 1024);
        ++histogram[pid];
    }
    assert(getc_unlocked(in) == EOF and "expected end-of-file");

    std::fclose(in);
    std::free(buf);
    return histogram;
}

histogram_t hist_mmap(const char *infile)
{
    /* Open the input file. */
    MMapFile in(infile);

    /* Access the data as array of struct. */
    auto data = reinterpret_cast<record*>(in.addr());
    const std::size_t num_records = in.size() / sizeof(*data);

    /* Compute the histogram. */
    std::array<unsigned, 1024> histogram{ 0 };
    for (auto p = data, end = data + num_records; p != end; ++p)
        ++histogram[p->get_radix_bits()];

    return histogram;
}

histogram_t hist_mmap_prefault(const char *infile)
{
    /* Open the input file. */
    MMapFile in(infile, O_RDONLY, true);

    /* Access the data as array of struct. */
    auto data = reinterpret_cast<record*>(in.addr());
    const std::size_t num_records = in.size() / sizeof(*data);

    /* Compute the histogram. */
    std::array<unsigned, 1024> histogram{ 0 };
    for (auto p = data, end = data + num_records; p != end; ++p)
        ++histogram[p->get_radix_bits()];

    return histogram;
}