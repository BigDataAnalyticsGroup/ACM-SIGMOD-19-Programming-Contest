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
#include "utility.hpp"
#include <algorithm>
#include <atomic>
#include <cassert>
#include <err.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <sstream>


histogram_t example_hist(const char *infile)
{
    constexpr int kTupleSize = 100;

    auto getPartitionId = [](const unsigned char *key) {
        return reinterpret_cast<const record*>(key)->get_radix_bits();
    };

    std::ifstream is(infile);
    if (!is) {
        std::cerr << "Could not open the file\n";
        std::exit(-1);
    }

    // get size of file
    is.seekg(0, is.end);
    const std::int64_t size = is.tellg();
    is.seekg(0);

    const std::int64_t num_tuples = size / kTupleSize;

    histogram_t histogram{ 0 };
    unsigned char buffer[kTupleSize];

    // read content of is
    for (std::int64_t i = 0; i < num_tuples; ++i) {
        is.read(reinterpret_cast<char*>(buffer), kTupleSize);
        const std::uint16_t p = getPartitionId(buffer);
        ++histogram[p];
    }

#ifndef NDEBUG
    {
        unsigned sum = 0;
        for (auto n : histogram)
            sum += n;
        assert(sum == num_tuples);
    }
#endif

    is.close();
    return histogram;
}

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
    histogram_t histogram{ 0 };
    for (unsigned i = 0; i != num_records; ++i) {
        read(fildes, buffer, sizeof buffer);
        const uint32_t pid = (uint32_t(buffer[0]) << 2) | (uint32_t(buffer[1]) >> 6);
        assert(pid < 1024);
        ++histogram[pid];
    }

#ifndef NDEBUG
    unsigned sum = 0;
    for (auto n : histogram)
        sum += n;
    assert(sum == num_records);
#endif

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
    histogram_t histogram{ 0 };
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

#ifndef NDEBUG
    unsigned sum = 0;
    for (auto n : histogram)
        sum += n;
    assert(sum == num_records);
#endif

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
    histogram_t histogram{ 0 };
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

#ifndef NDEBUG
    unsigned sum = 0;
    for (auto n : histogram)
        sum += n;
    assert(sum == num_records);
#endif

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
    histogram_t histogram{ 0 };
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

#ifndef NDEBUG
    unsigned sum = 0;
    for (auto n : histogram)
        sum += n;
    assert(sum == num_records);
#endif

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
    histogram_t histogram{ 0 };
    for (auto p = data, end = data + num_records; p != end; ++p)
        ++histogram[p->get_radix_bits()];

#ifndef NDEBUG
    unsigned sum = 0;
    for (auto n : histogram)
        sum += n;
    assert(sum == num_records);
#endif

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
    histogram_t histogram{ 0 };
    for (auto p = data, end = data + num_records; p != end; ++p)
        ++histogram[p->get_radix_bits()];

    return histogram;
}

histogram_t hist_mmap_MT(const char *infile, const unsigned num_threads)
{
    /* Lambda to compute the histogram of a range. */
    auto compute_hist = [](histogram_t *histogram, const record *begin, const record *end) {
        for (auto p = begin; p != end; ++p)
            ++(*histogram)[p->get_radix_bits()];
    };

    /* Open the input file. */
    MMapFile in(infile);

    /* Access the data as array of struct. */
    auto data = reinterpret_cast<record*>(in.addr());
    const std::size_t num_records = in.size() / sizeof(*data);

    /* Divide the input into chunks and allocate a histogram per chunk. */
    histogram_t *local_hists = new histogram_t[num_threads]{ {{0}} };
    std::thread *threads = new std::thread[num_threads];

    /* Spawn a thread per chunk to compute the local histogram. */
    std::size_t begin = 0;
    std::size_t end;
    const std::size_t step_size = num_records / num_threads;
    for (unsigned tid = 0; tid != num_threads - 1; ++tid) {
        end = begin + step_size;
        assert(begin >= 0);
        assert(end <= num_records);
        assert(begin < end);
        threads[tid] = std::thread(compute_hist, &local_hists[tid], data + begin, data + end);
        begin = end;
    }
    new (&threads[num_threads - 1]) std::thread(compute_hist, &local_hists[num_threads - 1], data + begin, data + num_records);

    /* Summarize the local histograms in a global histogram. */
    histogram_t the_histogram{ 0 };
    for (unsigned tid = 0; tid != num_threads; ++tid) {
        threads[tid].join();
        for (std::size_t i = 0; i != 1024; ++i)
            the_histogram[i] += local_hists[tid][i];
    }

#ifndef NDEBUG
    unsigned sum = 0;
    for (auto n : the_histogram)
        sum += n;
    assert(sum == num_records);
#endif

    delete []local_hists;
    delete []threads;
    return the_histogram;
}
