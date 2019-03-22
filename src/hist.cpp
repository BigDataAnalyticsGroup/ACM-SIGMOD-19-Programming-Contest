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
#include <thread>


std::ostream & operator<<(std::ostream &out, const histogram_t &histogram)
{
    unsigned sum = 0;
    for (auto n : histogram)
        sum += n;
    out << "Histogram of " << sum << " elements:\n";
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

#ifndef NDEBUG
    unsigned sum = 0;
    for (auto n : histogram)
        sum += n;
    assert(sum == num_records);
#endif

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
    histogram_t *local_hists = new histogram_t[num_threads]{ {{0}} };
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
    new (&threads[num_threads - 1]) std::thread(compute_hist, &local_hists[num_threads - 1], begin + partition_begin, end);

    /* Summarize the local histograms in a global histogram. */
    histogram_t the_histogram{ {0} };
    for (unsigned tid = 0; tid != num_threads; ++tid) {
        threads[tid].join();
        for (std::size_t i = 0; i != NUM_PARTITIONS; ++i)
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
