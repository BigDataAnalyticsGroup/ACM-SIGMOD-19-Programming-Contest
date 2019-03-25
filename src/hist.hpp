//===== hist.hpp =======================================================================================================
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

#pragma once

#include "record.hpp"
#include <array>
#include <iostream>


/** The histogram. */
struct histogram_t : public std::array<unsigned, NUM_PARTITIONS>
{
    /** Computes the total count of entries in the histogram. */
    unsigned count() const;
    /** Computes a checksum of the histogram. */
    unsigned checksum() const;

    friend std::ostream & operator<<(std::ostream &out, const histogram_t &histogram);
};

/** Implements histogram generation from an array of records. */
histogram_t hist(const record *begin, const record *end);
/** Implements histogram generation from an array of records. (Multi-threaded) */
histogram_t hist_MT(const record *begin, const record *end, const unsigned num_threads);

/** Generates a histogram of a record file using mmap(). */
histogram_t hist_from_file_mmap(const char *filename);
/** Generates a histogram of a record file using mmap(). (Multi-threaded) */
histogram_t hist_from_file_mmap_MT(const char *filename, const unsigned num_threads);

/** Generates a histogram of a record file using unbuffered reads. */
histogram_t hist_from_file_direct(const char *filename);

/** Generates a histogram of a record file using unbuffered reads. */
histogram_t hist_from_file_unbuffered(const char *filename);

histogram_t hist_from_file_buffered_default(const char *filename);

histogram_t hist_from_file_buffered_custom(const char *filename);

histogram_t hist_from_file_buffered_custom_MT(const char *filename, const unsigned num_threads);
