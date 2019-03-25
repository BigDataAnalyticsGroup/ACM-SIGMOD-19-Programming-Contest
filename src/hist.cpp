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
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <err.h>
#include <fcntl.h>
#include <thread>
#include <unistd.h>


constexpr std::size_t BLOCK_SIZE = 4096;


unsigned histogram_t::count() const
{
    unsigned sum = 0;
    for (auto n : *this)
        sum += n;
    return sum;
}

unsigned histogram_t::checksum() const
{
    unsigned checksum = 0;
    for (auto n : *this)
        checksum = checksum * 7 + n;
    return checksum;
}

std::ostream & operator<<(std::ostream &out, const histogram_t &histogram)
{
    out << "Histogram of " << histogram.count() << " elements:\n";
    for (std::size_t i = 0; i != histogram.size(); ++i) {
        if (histogram[i])
            out << "  " << i << ": " << histogram[i] << '\n';
    }
    return out;
}

histogram_t hist(const record *begin, const record *end)
{
    const std::size_t num_records = end - begin;

    /* Compute the histogram. */
    histogram_t histogram{ {0} };
    for (auto p = begin; p != end; ++p)
        ++histogram[p->get_radix_bits()];
    assert(histogram.count() == num_records);

    return histogram;
}

histogram_t hist_MT(const record *begin, const record *end, const unsigned num_threads)
{
    /* Lambda to compute the histogram of a range. */
    auto compute_hist = [](histogram_t *histogram, const record *begin, const record *end) {
        for (auto p = begin; p != end; ++p)
            ++(*histogram)[p->get_radix_bits()];
    };

    const std::size_t num_records = end - begin;

    /* Divide the input into chunks and allocate a histogram per chunk. */
    histogram_t *local_hists = new histogram_t[num_threads]{ {{{0}}} };
    std::thread *threads = new std::thread[num_threads];

    /* Spawn a thread per chunk to compute the local histogram. */
    std::size_t partition_begin = 0;
    std::size_t partition_end;
    const std::size_t step_size = num_records / num_threads;
    for (unsigned tid = 0; tid != num_threads - 1; ++tid) {
        partition_end = partition_begin + step_size;
        assert(partition_begin >= 0);
        assert(partition_end <= num_records);
        assert(partition_begin < partition_end);
        threads[tid] = std::thread(compute_hist, &local_hists[tid], begin + partition_begin, begin + partition_end);
        partition_begin = partition_end;
    }
    threads[num_threads - 1] = std::thread(compute_hist, &local_hists[num_threads - 1], begin + partition_begin, end);

    /* Summarize the local histograms in a global histogram. */
    histogram_t the_histogram{ {0} };
    for (unsigned tid = 0; tid != num_threads; ++tid) {
        threads[tid].join();
        for (std::size_t i = 0; i != NUM_PARTITIONS; ++i)
            the_histogram[i] += local_hists[tid][i];
    }
    assert(the_histogram.count() == num_records);

    delete []local_hists;
    delete []threads;
    return the_histogram;
}

histogram_t hist_from_file_mmap(const char *filename)
{
    MMapFile file(filename);

    auto arr = static_cast<record*>(file.addr());
    const auto size = file.size() / sizeof(*arr);

    return hist(arr, arr + size);
}

histogram_t hist_from_file_mmap_MT(const char *filename, const unsigned num_threads)
{
    MMapFile file(filename);

    auto arr = static_cast<record*>(file.addr());
    const auto size = file.size() / sizeof(*arr);

    return hist_MT(arr, arr + size, num_threads);
}

histogram_t hist_from_file_direct(const char *filename)
{
    (void) filename;
    histogram_t histogram{ {0} };
#if 0
    constexpr std::size_t BLOCK_SIZE = 4096;
    constexpr std::size_t BUFFER_SIZE = sizeof(record) * BLOCK_SIZE;

    int fd = open(filename, O_RDONLY|O_DIRECT);
    if (fd == -1)
        err(EXIT_FAILURE, "Could not open file '%s'", filename);

    if (posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL))
        warn("fadvise() failed");

    void *buffer = std::aligned_alloc(BLOCK_SIZE, BLOCK_SIZE);
    while (int ret = read(fd, buffer, BLOCK_SIZE)) {
        if (ret == -1)
            err(EXIT_FAILURE, "Failed to read from file '%s'", filename);
        //for (std::size_t i = 0, end = ret / sizeof(record); i != end; ++i)
            //++histogram[buffer[i].get_radix_bits()];
    }

    free(buffer);
    close(fd);
#endif
    return histogram;
}

histogram_t hist_from_file_unbuffered(const char *filename)
{
    histogram_t histogram{ {0} };

    int fd = open(filename, O_RDONLY);
    if (fd == -1)
        err(EXIT_FAILURE, "Could not open file '%s'", filename);

    if (posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL))
        warn("fadvise() failed");

    record buffer;
    while (int ret = read(fd, &buffer, sizeof(buffer))) {
        if (ret != sizeof(buffer))
            err(EXIT_FAILURE, "Reading next record failed and returned %d", ret);
        ++histogram[buffer.get_radix_bits()];
    }

    close(fd);
    return histogram;
}

histogram_t hist_from_file_buffered_default(const char *filename)
{
    histogram_t histogram{ {0} };

    FILE *file = fopen(filename, "rb");
    if (not file)
        err(EXIT_FAILURE, "Could not open file '%s'", filename);

    if (posix_fadvise(fileno(file), 0, 0, POSIX_FADV_SEQUENTIAL))
        warn("fadvise() failed");

    record buffer;
    while (fread(&buffer, sizeof(buffer), 1, file) == 1)
        ++histogram[buffer.get_radix_bits()];

    fclose(file);
    return histogram;
}

histogram_t hist_from_file_buffered_custom(const char *filename)
{
    histogram_t histogram{ {0} };

    FILE *file = fopen(filename, "rb");
    if (not file)
        err(EXIT_FAILURE, "Could not open file '%s'", filename);

    if (posix_fadvise(fileno(file), 0, 0, POSIX_FADV_SEQUENTIAL))
        warn("fadvise() failed");

    constexpr std::size_t BUFFER_SIZE = 64 * BLOCK_SIZE;
    char *fbuffer = reinterpret_cast<char*>(std::aligned_alloc(BLOCK_SIZE, BUFFER_SIZE));
    setvbuf(file, fbuffer, _IOFBF, BUFFER_SIZE);

    record buffer;
    while (fread(&buffer, sizeof(buffer), 1, file) == 1)
        ++histogram[buffer.get_radix_bits()];

    free(fbuffer);
    fclose(file);
    return histogram;
}

histogram_t hist_from_file_buffered_custom_MT(const char *filename, const unsigned num_threads)
{
    int fd = open(filename, O_RDONLY);
    if (fd == -1)
        err(EXIT_FAILURE, "Could not open file '%s'", filename);

    struct stat statbuf;
    if (fstat(fd, &statbuf))
        err(EXIT_FAILURE, "Could not get status of file '%s'", filename);
    close(fd);
    const std::size_t size_in_bytes = statbuf.st_size;
    const std::size_t num_records = size_in_bytes / sizeof(record);

    /* Lambda to compute the histogram of a range. */
    auto compute_hist = [filename](histogram_t *histogram, const std::size_t offset, const std::size_t count) {
        constexpr std::size_t BUFFER_SIZE = 64 * BLOCK_SIZE;
        FILE *file = fopen(filename, "r"); // do not close
        if (not file)
            err(EXIT_FAILURE, "Failed to create stream for file '%s'", filename);
        char *fbuffer = reinterpret_cast<char*>(std::aligned_alloc(BLOCK_SIZE, BUFFER_SIZE));
        setvbuf(file, fbuffer, _IOFBF, BUFFER_SIZE);
        fseek(file, offset, SEEK_SET);

        record buffer;
        for (std::size_t i = 0; i != count; ++i) {
            if (fread(&buffer, sizeof(buffer), 1, file) != 1)
                err(EXIT_FAILURE, "Failed to read from file");
            ++(*histogram)[buffer.get_radix_bits()];
        }
        assert(histogram->count() == count);

        free(fbuffer);
        fclose(file);
    };

    /* Divide the input into chunks and allocate a histogram per chunk. */
    histogram_t *local_hists = new histogram_t[num_threads]{ {{{0}}} };
    std::thread *threads = new std::thread[num_threads];

    /* Spawn a thread per chunk to compute the local histogram. */
    const std::size_t step_size = std::ceil(double(num_records) / num_threads);
    for (unsigned tid = 0; tid != num_threads; ++tid) {
        const auto offset = tid * step_size * sizeof(record);
        assert(offset < size_in_bytes and "offset out of bounds");
        const auto count = std::min(step_size, (size_in_bytes - offset) / sizeof(record));
        assert(count <= step_size and "count out of bounds");
        assert(offset + count * sizeof(record) <= size_in_bytes and "offset + count out of bounds");
        threads[tid] = std::thread(compute_hist, &local_hists[tid], offset, count);
    }

    /* Summarize the local histograms in a global histogram. */
    histogram_t the_histogram{ {0} };
    for (unsigned tid = 0; tid != num_threads; ++tid) {
        threads[tid].join();
        for (std::size_t i = 0; i != NUM_PARTITIONS; ++i)
            the_histogram[i] += local_hists[tid][i];
    }
    assert(the_histogram.count() == num_records);

    delete []local_hists;
    delete []threads;
    return the_histogram;
}
