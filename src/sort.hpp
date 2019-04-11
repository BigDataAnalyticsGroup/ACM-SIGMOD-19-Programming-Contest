//===== sort.hpp =======================================================================================================
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

#pragma once

#include "hist.hpp"
#include "record.hpp"


namespace {

/** Computes the power of base b to the exponent e. */
template<typename Base, typename Exponent>
constexpr auto pow(Base b, Exponent e) -> decltype(b * e)
{
    return e ? b * pow(b, e - 1) : 1;
}

}

/** The number of buckets for a radix sort with a byte as digit. */
constexpr std::size_t NUM_BUCKETS = pow(2, 8lu * sizeof(decltype(record::key)::value_type));


/** Computes a histogram of the records for the given digit. */
histogram_t<unsigned, NUM_BUCKETS> compute_histogram(const record * const first,
                                                     const record * const last,
                                                     const unsigned digit);

/** Computes the bucket locations given the histogram. */
std::array<record*, NUM_BUCKETS> compute_buckets(record * const first,
                                                 record * const last,
                                                 const histogram_t<unsigned, NUM_BUCKETS> &histogram);

/** Implements the partitioning phase of American Flag Sort, where items are distributed to their buckets. */
void american_flag_sort_partitioning(const unsigned digit,
                                     const histogram_t<unsigned, NUM_BUCKETS> &histogram,
                                     const std::array<record*, NUM_BUCKETS> &buckets);

/** Implements American Flag Sort of records.  Processes the key byte-wise. */
void american_flag_sort(record *first, record *last, const unsigned digit = 0);

/** Implements American Flag Sort of records.  Processes the key byte-wise.  Performs multi-threading on first
 * recursion. */
void american_flag_sort_MT(record * const first, record * const last,
                           const unsigned num_threads, const unsigned digit = 0);

/** Like american_flag_sort_MT, but exploits parallelism from the very beginning. */
void american_flag_sort_parallel(record * const first, record * const last, const unsigned digit);

/** Implements Selection Sort.  Finds the smallest item in the remaining unsorted sequence, moves it to the front, and
 * continues at the next item.  */
void selection_sort(record *first, record *last);

/** Implements Insertion Sort. */
void insertion_sort(record *first, record *last);

/** Performs a simple American Flag Sort and falls back to std::sort for small ranges. */
void my_hybrid_sort(record *first, record *last);

/** Performs a simple American Flag Sort and falls back to std::sort for small ranges. */
void my_hybrid_sort_MT(record *first, record *last);
