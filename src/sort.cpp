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
#include <chrono>
#include <cstdint>
#include <cstring>
#include <err.h>
#include <parallel/algorithm>
#include <sched.h>
#include <sched.h>
#include <thread>
#include <thread>
#include <vector>


#ifndef NDEBUG
#define VERBOSE
#endif


#ifdef SUBMISSION
constexpr unsigned NUM_SOCKETS = 2;
constexpr unsigned NUM_HW_CORES_PER_SOCKET = 10;
constexpr unsigned NUM_THREADS_PER_HW_CORE = 2;
constexpr unsigned NUM_LOGICAL_CORES_PER_SOCKET = NUM_HW_CORES_PER_SOCKET * NUM_THREADS_PER_HW_CORE;
constexpr unsigned NUM_LOGICAL_CORES_TOTAL = NUM_LOGICAL_CORES_PER_SOCKET * NUM_SOCKETS;

constexpr unsigned NUM_THREADS_HISTOGRAM = NUM_LOGICAL_CORES_TOTAL;
constexpr unsigned NUM_THREADS_PARTITIONING = NUM_LOGICAL_CORES_PER_SOCKET;
constexpr unsigned NUM_THREADS_RECURSION = NUM_LOGICAL_CORES_TOTAL;
#else
constexpr unsigned NUM_THREADS_HISTOGRAM = 8;
constexpr unsigned NUM_THREADS_PARTITIONING = 8;
constexpr unsigned NUM_THREADS_RECURSION = 8;
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

/** Compute a global histogram by computing a histogram for each thread region. */
histogram_t<unsigned, NUM_BUCKETS> compute_histogram_parallel(const record * const first,
                                                              const record * const last,
                                                              const unsigned digit,
                                                              const unsigned num_threads)
{
    const auto num_records = last - first;
    auto histograms = new histogram_t<unsigned, NUM_BUCKETS>[num_threads];
    auto compute_hist = [histograms, digit](unsigned tid, const record *first, const record *last) {
        histograms[tid] = compute_histogram(first, last, digit);
    };

    auto threads = new std::thread[num_threads];
    const auto num_records_per_thread = num_records / num_threads;
    auto thread_first = first;
    for (unsigned tid = 0; tid != num_threads; ++tid) {
        auto thread_last = tid == num_threads - 1 ? last : thread_first + num_records_per_thread;
        assert(thread_first >= first);
        assert(thread_first <= thread_last);
        assert(thread_last <= last);
        threads[tid] = std::thread(compute_hist, tid, thread_first, thread_last);
        thread_first = thread_last;
    }

    /* Accumulate the histograms. */
    histogram_t<unsigned, NUM_BUCKETS> histogram{ {0} };
    for (unsigned tid = 0; tid != num_threads; ++tid) {
        threads[tid].join();
        histogram += histograms[tid];
    }
    assert(histogram.count() == num_records and "incorrect histogram accumulation");

    delete[] histograms;
    delete[] threads;
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

bool is_partitioned(const record *first,
                    const histogram_t<unsigned, NUM_BUCKETS> &histogram,
                    const unsigned digit)
{
    const record *p = first;
    for (std::size_t bucket_id = 0; bucket_id != NUM_BUCKETS; ++bucket_id) {
        for (const record *end = p + histogram[bucket_id]; p != end; ++p) {
            if (p->key[digit] != bucket_id)
                return false;
        }
    }
    return true;
}

void american_flag_sort_partitioning(const histogram_t<unsigned, NUM_BUCKETS> &histogram,
                                     const std::array<record*, NUM_BUCKETS> &buckets,
                                     const unsigned digit)
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

void american_flag_sort_partitioning_parallel(const histogram_t<unsigned, NUM_BUCKETS> &histogram,
                                              const std::array<record*, NUM_BUCKETS> &buckets,
                                              const unsigned digit,
                                              const unsigned num_threads)
{
    /* Compute the heads and tails of the buckets.  Place the heads in separate cache lines, that are guarded with
     * std::atomic for inter-thread synchronization. */
    struct __attribute__((aligned(64))) ptr_t { std::atomic<record*> ptr; };
    static_assert(sizeof(ptr_t) == 64, "incorrect size; could lead to incorrect alignment");
    ptr_t *heads = static_cast<ptr_t*>(aligned_alloc(64, NUM_BUCKETS * sizeof(ptr_t)));
    ptr_t *tails = static_cast<ptr_t*>(aligned_alloc(64, NUM_BUCKETS * sizeof(ptr_t)));
    for (std::size_t i = 0; i != NUM_BUCKETS; ++i) {
        new (&heads[i]) std::atomic<record*>(buckets[i]);
        new (&tails[i]) std::atomic<record*>(buckets[i] + histogram[i]);
    }

    std::atomic_uint_fast32_t bucket_counter(0);
    auto distribute = [digit, heads, tails, histogram, &bucket_counter]() {
        using std::swap;
        std::ostringstream oss;

        unsigned curr_bucket;
        while ((curr_bucket = bucket_counter.fetch_add(1)) < NUM_BUCKETS) {
            if (not histogram[curr_bucket]) continue; // skip empty buckets

            /* Acquire next element from current bucket. */
            auto src = heads[curr_bucket].ptr++;

            for (;;) {
                /* Check whether we finished the bucket. */
                if (src >= tails[curr_bucket].ptr.load()) { // this slot was or will be written by another thread
                    heads[curr_bucket].ptr.fetch_sub(1); // reset own head pointer to the src slot
                    goto bucket_finished; // we are done with this bucket
                }

                /* Compute destination bucket. */
                unsigned dst_bucket = src->key[digit];

                /* If the element is already in the right bucket, skip it. */
                if (dst_bucket == curr_bucket) {
                    src = heads[curr_bucket].ptr++;
                    continue;
                }

                /* Acquire space in the destination bucket. */
                auto dst = --tails[dst_bucket].ptr;

                /* Check that the space in the destination bucket is free to swap. */
                if (dst < heads[dst_bucket].ptr.load()) { // this slot was or will be written by the bucket owner
                    heads[curr_bucket].ptr.fetch_sub(1); // reset own head
                    tails[dst_bucket].ptr.fetch_add(1); // reset destination tail
                    return; // we are done with this bucket
                }

                swap(*src, *dst);
            }
bucket_finished:;
        }
    };

    auto threads = new std::thread[num_threads];
    for (unsigned tid = 0; tid != NUM_THREADS_PARTITIONING; ++tid)
        threads[tid] = std::thread(distribute);
    for (unsigned tid = 0; tid != NUM_THREADS_PARTITIONING; ++tid)
        threads[tid].join();
    delete[] threads;

    /* There can be at most one incorrect item per bucket.  Go through all buckets, see if there is an incorrect item,
     * and rotate it to its correct position in American Flag Sort style.  Repeat until the bucket is correct.  Proceed
     * with next bucket. */
    for (std::size_t curr_bucket = 0; curr_bucket != NUM_BUCKETS; ++curr_bucket) {
        using std::swap;

        auto src = heads[curr_bucket].ptr.fetch_add(1, std::memory_order_relaxed);
        while (src < tails[curr_bucket].ptr.load()) {
            auto dst_bucket = src->key[digit];
            if (dst_bucket == curr_bucket) {
                src = heads[curr_bucket].ptr.fetch_add(1, std::memory_order_relaxed);
                continue;
            }
            auto dst = heads[dst_bucket].ptr.fetch_add(1, std::memory_order_relaxed);
            swap(*src, *dst);
        }
    }

    free(heads);
    free(tails);

    assert(is_partitioned(buckets[0], histogram, digit));
}

void american_flag_sort(record * const first, record * const last, const unsigned digit)
{
    const auto histogram = compute_histogram(first, last, digit);
    const auto buckets = compute_buckets(first, last, histogram);
    american_flag_sort_partitioning(histogram, buckets, digit);

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
    american_flag_sort_partitioning(histogram, buckets, digit);

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
    american_flag_sort_partitioning(histogram, buckets, digit);

    /* Recursive descent to sort buckets. */
    const auto next_digit = digit + 1; ///< next digit to sort by
    if (next_digit != 10) {
        auto p = first;
        for (auto n : histogram) {
            if (n > AMERICAN_FLAG_SORT_MIN_SIZE)
                my_hybrid_sort_helper(p, p + n, next_digit);
            else
                std::sort(p, p + n);
                //selection_sort(p, p + n);
            p += n;
        }
    }
}

void my_hybrid_sort_MT(record * const first, record * const last)
{
    const auto histogram = compute_histogram(first, last, 0);
    const auto buckets = compute_buckets(first, last, histogram);
    american_flag_sort_partitioning(histogram, buckets, 0);

    /* Recursively sort the buckets.  Use a thread pool of worker threads and let the workers claim buckets for
     * sorting from a queue. */
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
                if (num_records > AMERICAN_FLAG_SORT_MIN_SIZE)
                    my_hybrid_sort_helper(thread_first, thread_last, 1);
                else
                    std::sort(thread_first, thread_last);
            }
        }
    };

    std::array<std::thread, NUM_THREADS_RECURSION> threads;
    for (unsigned tid = 0; tid != NUM_THREADS_RECURSION; ++tid)
        threads[tid] = std::thread(recurse);
    for (unsigned tid = 0; tid != NUM_THREADS_RECURSION; ++tid)
        threads[tid].join();
}

void my_hybrid_sort(record *first, record *last) { my_hybrid_sort_helper(first, last, 0); }

void american_flag_sort_parallel(record * const first, record * const last, const unsigned digit)
{
    using namespace std::chrono;

    const auto histogram = compute_histogram_parallel(first, last, digit, NUM_THREADS_HISTOGRAM);
    const auto buckets = compute_buckets(first, last, histogram);

#ifdef SUBMISSION
    /* By evaluation, we figured that logical cores 0-9,20-29 belong to NUMA region 0.  Explicitly bind this process to
     * these logical cores to avoid NUMA.  */
    {
        cpu_set_t *cpus = CPU_ALLOC(40);
        if (not cpus)
            err(EXIT_FAILURE, "Failed to allocate CPU_SET of 20 CPUs on socket 0");
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

    /* Concurrently distribute items into buckets. */
    american_flag_sort_partitioning_parallel(histogram, buckets, digit, NUM_THREADS_PARTITIONING);

#ifdef SUBMISSION
    {
        cpu_set_t *cpus = CPU_ALLOC(40);
        if (not cpus)
            err(EXIT_FAILURE, "Failed to allocate CPU_SET of 40 CPUs");
        const auto size = CPU_ALLOC_SIZE(40);
        CPU_ZERO_S(size, cpus);
        for (int cpu = 0; cpu != NUM_LOGICAL_CORES_TOTAL; ++cpu)
            CPU_SET_S(cpu, size, cpus);
        assert(CPU_COUNT_S(size, cpus) == NUM_LOGICAL_CORES_TOTAL and "allocated incorrect number of logical CPUs");

        /* Bind process and all children to the desired logical CPUs. */
        sched_setaffinity(0 /* this thread */, size, cpus);

        CPU_FREE(cpus);
    }
#endif

    /* Recursively sort the buckets.  Use a thread pool of worker threads and let the workers claim buckets for
     * sorting from a queue. */
    const auto next_digit = digit + 1; ///< next digit to sort by
    if (next_digit != 10) {
#if 0 // parallelize among the buckets
        std::atomic_uint_fast32_t bucket_counter(0);
        auto recurse = [&]() {
            unsigned bucket_id;
            while ((bucket_id = bucket_counter.fetch_add(1)) < NUM_BUCKETS) {
                const auto num_records = histogram[bucket_id];
                if (num_records > 1) {
                    auto thread_first = buckets[bucket_id];
                    auto thread_last = thread_first + num_records;
                    assert(first <= thread_first);
                    assert(thread_last <= last);
                    assert(thread_first <= thread_last);
                    if (num_records > AMERICAN_FLAG_SORT_MIN_SIZE)
                        my_hybrid_sort_helper(thread_first, thread_last, next_digit);
                    else
                        std::sort(thread_first, thread_last);
                }
            }
        };

        std::array<std::thread, NUM_THREADS_RECURSION> threads;
        for (unsigned tid = 0; tid != NUM_THREADS_RECURSION; ++tid)
            threads[tid] = std::thread(recurse);
        for (unsigned tid = 0; tid != NUM_THREADS_RECURSION; ++tid)
            threads[tid].join();
#else // parallelize within the buckets
        for (std::size_t bucket_id = 0; bucket_id != NUM_BUCKETS; ++bucket_id) {
            const auto num_records = histogram[bucket_id];
            const auto bucket_first = buckets[bucket_id];
            const auto bucket_last = bucket_first + num_records;
            if (num_records > 2000)
                american_flag_sort_parallel(bucket_first, bucket_last, next_digit);
            else if (num_records > 500)
                __gnu_parallel::sort(bucket_first, bucket_last);
            else if (num_records)
                std::sort(bucket_first, bucket_last);

        }
#endif
    }
}
