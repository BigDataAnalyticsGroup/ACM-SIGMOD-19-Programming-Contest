//===== sort.cpp =======================================================================================================
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
//      This file provides algorithms to sort data on disk, generated by "gensort" from the sortbenchmark.org
//      benchmarks.
//
//======================================================================================================================

#include "sort.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <thread>
#include <vector>


#ifndef NDEBUG
#define VERBOSE
#endif


/** The minimum size of a sequence for American Flag Sort.  For shorter sequences, use std::sort or else. */
//constexpr std::size_t AMERICAN_FLAG_SORT_MIN_SIZE = 1UL << 11;
constexpr std::size_t AMERICAN_FLAG_SORT_MIN_SIZE = 50;

histogram_t<unsigned, NUM_BUCKETS> compute_histogram(const record * const first,
                                                     const record * const last,
                                                     const unsigned digit)
{
    histogram_t<unsigned, NUM_BUCKETS> histogram{ {0} };
    for (auto p = first; p != last; ++p)
        ++histogram[p->key[digit]];
    assert(histogram.count() == last - first and "histogram has incorrect number of entries");
    return histogram;
}

std::array<record*, NUM_BUCKETS> compute_buckets(record * const first,
                                                 record * const last,
                                                 const histogram_t<unsigned, NUM_BUCKETS> &histogram)
{
    std::array<record*, NUM_BUCKETS> buckets;
    auto p = first;
    for (std::size_t i = 0; i != NUM_BUCKETS; ++i) {
        buckets[i] = p;
        p += histogram[i];
    }
    (void) last, assert(p == last and "incorrect computation of buckets");
    return buckets;
}

void american_flag_sort_partitioning(const unsigned digit,
                                     const histogram_t<unsigned, NUM_BUCKETS> &histogram,
                                     const std::array<record*, NUM_BUCKETS> &buckets)
{
    using std::swap;
    auto runners = buckets;
    unsigned current_bucket = 0; ///< bucket id of the current source bucket
    while (not histogram[current_bucket]) ++current_bucket; // find the first non-empty bucket
    for (;;) {
        auto src = runners[current_bucket]; // get source address
        const auto dst_bucket = src->key[digit]; // get destination bucket by digit
        auto dst = runners[dst_bucket]++; // get destination address

        if (src == dst) {
            /* Item is already in its destination bucket and need not be moved.  Find new source by searching for the
             * first unfinished bucket, starting with the current bucket. */
            while (runners[current_bucket] == buckets[current_bucket + 1]) {
                ++current_bucket;
                if (current_bucket == NUM_BUCKETS - 1)
                    return; // all buckets are finished
            }
            src = runners[current_bucket];
            continue;
        }

        swap(*src, *dst); // swap items
    }
}

void american_flag_sort(record * const first, record * const last, const unsigned digit)
{
    const auto histogram = compute_histogram(first, last, digit);
    const auto buckets = compute_buckets(first, last, histogram);
    american_flag_sort_partitioning(digit, histogram, buckets);

    /* Recursive descent to sort buckets. */
    const auto next_digit = digit + 1; ///< next digit to sort by
    if (next_digit != 10) {
        auto p = first;
        for (auto n : histogram) {
            if (n > 1)
                american_flag_sort(p, p + n, next_digit);
            p += n;
        }
    }
}

void american_flag_sort_MT(record * const first, record * const last,
                           const unsigned num_threads, const unsigned digit)
{
    const auto histogram = compute_histogram(first, last, digit);
    const auto buckets = compute_buckets(first, last, histogram);
    american_flag_sort_partitioning(digit, histogram, buckets);

    /* Recursively sort the buckets.  Use a thread pool of worker threads and let the workers claim buckets for
     * sorting from a queue. */
    const auto next_digit = digit + 1; ///< next digit to sort by
    if (next_digit != 10) {
        std::atomic_uint_fast32_t bucket_counter(0);
        auto recurse = [&]() {
            uint_fast32_t bucket_id;
            while ((bucket_id = bucket_counter.fetch_add(1)) < NUM_BUCKETS) {
                const auto num_records = histogram[bucket_id];
                if (num_records > 1) {
                    auto thread_first = buckets[bucket_id];
                    auto thread_last = thread_first + num_records;
                    assert(first <= thread_first);
                    assert(thread_last <= last);
                    assert(thread_first <= thread_last);
                    american_flag_sort(thread_first, thread_last, next_digit);
                }
            }
        };

        auto threads = new std::thread[num_threads];
        for (unsigned tid = 0; tid != num_threads; ++tid)
            threads[tid] = std::thread(recurse);
        for (unsigned tid = 0; tid != num_threads; ++tid) {
            if (threads[tid].joinable())
                threads[tid].join();
        }
    }
}

void american_flag_sort_parallel(record * const first, record * const last,
                                 const unsigned digit, ctpl::thread_pool &thread_pool)
{
    const unsigned num_threads = thread_pool.size();
    auto histograms = new histogram_t<unsigned, NUM_BUCKETS>[num_threads];
    auto compute_hist = [histograms](int, int tid, record *first, record *last) {
        histograms[tid] = compute_histogram(first, last, 0);
    };

    /* Compute a histogram for each thread region. */
    const auto num_records = last - first;
    const auto num_records_per_thread = num_records / num_threads;
    auto thread_first = first;
    auto results = new std::future<void>[num_threads];
    for (unsigned tid = 0; tid != num_threads; ++tid) {
        auto thread_last = tid == num_threads - 1 ? last : thread_first + num_records_per_thread;
        assert(thread_first >= first);
        assert(thread_first <= thread_last);
        assert(thread_last <= last);
        results[tid] = thread_pool.push(compute_hist, tid, thread_first, thread_last);
        thread_first = thread_last;
    }
    for (unsigned tid = 0; tid != num_threads; ++tid)
        results[tid].get();

    /* Merge the histograms and compute the buckets. */
    histogram_t<unsigned, NUM_BUCKETS> histogram{ {0} };
    for (unsigned tid = 0; tid != num_threads; ++tid)
        histogram += histograms[tid];
    const auto buckets = compute_buckets(first, last, histogram);
    assert(histogram.count() == num_records and "histogram accumulation failed");

    std::cerr << histogram << std::endl;

    /* Compute the heads and tails of the buckets.  Place the heads in separate cache lines, that are guarded with
     * std::atomic for inter-thread synchronization. */
    struct __attribute__((aligned(64))) head_t { std::atomic<record*> ptr; };
    static_assert(sizeof(head_t) == 64, "incorrect size; could lead to incorrect alignment");
    head_t *heads = static_cast<head_t*>(aligned_alloc(64, NUM_BUCKETS * sizeof(head_t)));
    std::array<record*, NUM_BUCKETS> tails;
    for (std::size_t i = 0; i != NUM_BUCKETS; ++i) {
        new (&heads[i]) std::atomic<record*>(buckets[i]);
        tails[i] = buckets[i] + histogram[i];
    }

    unsigned current_bucket = 0; ///< bucket id of the current source bucket
    while (not histogram[current_bucket]) ++current_bucket; // find the first non-empty bucket

    auto distribute = [first, digit, heads, tails](int, int tid) {
        using std::swap;
        std::ostringstream oss;

        unsigned current_bucket = 0;
        record *src = heads[current_bucket].ptr.fetch_add(1); // claim a record in the source bucket
        for (;;) {
            /* If current source is outside the current bucket, search for a new source. */
            if (src >= tails[current_bucket]) { // out-of-bounds
#ifndef NDEBUG
                oss.str("");
                oss << "  Thread " << tid << ": Current src at offset " << (src - first) << " is out of bounds.\n";
                std::cerr << oss.str();
#endif
                while (src >= tails[current_bucket]) {
                    --heads[current_bucket].ptr; // give claimed record back
                    ++current_bucket;
                    if (current_bucket == NUM_BUCKETS) return; // all buckets finished
                    src = heads[current_bucket].ptr.fetch_add(1);
                }
#ifndef NDEBUG
                oss.str("");
                oss << "  Thread " << tid << ": Continue with bucket " << current_bucket << " and offset "
                    << (src - first) << ".\n";
                std::cerr << oss.str();
#endif
            }
            assert(src < tails[current_bucket] and "source out of bounds");

#ifndef NDEBUG
            oss.str("");
            oss << "  Thread " << tid << ": Load item ";
            src->key.to_ascii(oss);
            oss << " at offset " << (src - first) << ".\n";
            std::cerr << oss.str();
#endif

            const auto dst_bucket = src->key[digit]; // get destination bucket by digit
#ifndef NDEBUG
            oss.str("");
            oss << "  Thread " << tid << ": Item ";
            src->key.to_ascii(oss);
            oss << " belongs into bucket " << unsigned(dst_bucket) << ".\n";
            std::cerr << oss.str();
#endif
            if (dst_bucket == current_bucket) {
                src = heads[current_bucket].ptr.fetch_add(1);
                continue;
            }

            auto dst = heads[dst_bucket].ptr.fetch_add(1); // get destination address
#ifndef NDEBUG
            oss.str("");
            oss << "  Thread " << tid << ": Swap item ";
            src->key.to_ascii(oss);
            oss << " at offset " << (src - first) << " with item ";
            dst->key.to_ascii(oss);
            oss << " at offset " << (dst - first) << " in bucket " << unsigned(dst_bucket) << ".\n";
            std::cerr << oss.str();
#endif
            if (dst >= tails[dst_bucket]) {
                oss.str("");
                oss << "  Thread " << tid << ": Deadlock detected.  Holding resource at offset " << (src - first)
                    << " and trying to acquire resource in bucket " << unsigned(dst_bucket)
                    << ", which is currently at offset " << (dst - first) << " and out of bounds.\n";
                std::cerr << oss.str();
            }
            assert(dst < tails[dst_bucket] and "destination out-of-bounds; this must never happen; histogram incorrect or sync error?");

            swap(*src, *dst); // swap items
        }
    };

    for (unsigned tid = 0; tid != 2; ++tid)
        results[tid] = thread_pool.push(distribute, tid);
    for (unsigned tid = 0; tid != 2; ++tid)
        results[tid].get();

    delete[] results;
    free(heads);

    /* Recursively sort the buckets.  Use a thread pool of worker threads and let the workers claim buckets for
     * sorting from a queue. */
    const auto next_digit = digit + 1; ///< next digit to sort by
    if (next_digit != 10) {
        std::atomic_uint_fast32_t bucket_counter(0);
        auto recurse = [&]() {
            uint_fast32_t bucket_id;
            while ((bucket_id = bucket_counter.fetch_add(1)) < NUM_BUCKETS) {
                const auto num_records = histogram[bucket_id];
                if (num_records > 1) {
                    auto thread_first = buckets[bucket_id];
                    auto thread_last = thread_first + num_records;
                    assert(first <= thread_first);
                    assert(thread_last <= last);
                    assert(thread_first <= thread_last);
                    american_flag_sort(thread_first, thread_last, next_digit);
                }
            }
        };

        auto threads = new std::thread[num_threads];
        for (unsigned tid = 0; tid != num_threads; ++tid)
            threads[tid] = std::thread(recurse);
        for (unsigned tid = 0; tid != num_threads; ++tid) {
            if (threads[tid].joinable())
                threads[tid].join();
        }
    }
}

void selection_sort(record *first, record *last)
{
    using std::swap;

    /* For every position in the sequence, search for the next smallest item in the remaining, unsorted sequence. */
    for (record *current = first; current != last; ++current) {
        record *min = current;
        /* Starting at the current item, find the smallest item in the remaining sequence. */
        for (auto runner = current + 1; runner < last; ++runner) {
            if (*runner < *min)
                min = runner;
        }
        assert(*min <= *current);
        swap(*current, *min);
    }
}

void insertion_sort(record *first, record *last)
{
    using std::swap;

    record tmp, *p, *q;
    for (p = first + 1; p < last; ++p) {
        tmp = *p;
        for (q = p; q > first and *(q-1) > tmp; --q)
            *q = *(q-1);
        *q = tmp;
    }
}

/** Performs a simple American flag sort and falls back to std::sort for small ranges. */
void my_hybrid_sort_helper(record * const first, record * const last, const unsigned digit)
{
    const auto histogram = compute_histogram(first, last, digit);
    const auto buckets = compute_buckets(first, last, histogram);
    american_flag_sort_partitioning(digit, histogram, buckets);

    /* Recursive descent to sort buckets. */
    const auto next_digit = digit + 1; ///< next digit to sort by
    if (next_digit != 10) {
        auto p = first;
        for (auto n : histogram) {
            if (n > AMERICAN_FLAG_SORT_MIN_SIZE)
                my_hybrid_sort_helper(p, p + n, next_digit);
            else
                //std::sort(p, p + n);
                selection_sort(p, p + n);
            p += n;
        }
    }
}

void my_hybrid_sort_MT(record * const first, record * const last, ctpl::thread_pool &thread_pool)
{
    const unsigned num_threads = thread_pool.size();
    const auto histogram = compute_histogram(first, last, 0);
    const auto buckets = compute_buckets(first, last, histogram);
    american_flag_sort_partitioning(0, histogram, buckets);

    /* Recursively sort the buckets.  Use a thread pool of worker threads and let the workers claim buckets for
     * sorting from a queue. */
    std::atomic_uint_fast32_t bucket_counter(0);
    auto recurse = [&](int) {
        uint_fast32_t bucket_id;
        while ((bucket_id = bucket_counter.fetch_add(1)) < NUM_BUCKETS) {
            const auto num_records = histogram[bucket_id];
            if (num_records > 1) {
                auto thread_first = buckets[bucket_id];
                auto thread_last = thread_first + num_records;
                assert(first <= thread_first);
                assert(thread_last <= last);
                assert(thread_first <= thread_last);
                if (num_records > AMERICAN_FLAG_SORT_MIN_SIZE)
                    my_hybrid_sort_helper(thread_first, thread_last, 1);
                else
                    std::sort(thread_first, thread_last);
            }
        }
    };

    auto results = new std::future<void>[num_threads];
    for (unsigned tid = 0; tid != num_threads; ++tid)
        results[tid] = thread_pool.push(recurse);
    for (unsigned tid = 0; tid != num_threads; ++tid)
        results[tid].get();
    delete[] results;
}

void my_hybrid_sort(record *first, record *last) { my_hybrid_sort_helper(first, last, 0); }
