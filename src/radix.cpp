//===== radix.cpp ======================================================================================================
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
//      This file provides algorithms to radix partition data on disk, generated by "gensort" from the sortbenchmark.org
//      benchmarks.
//
//======================================================================================================================

#include "radix.hpp"

#include "hist.hpp"
#include "mmap.hpp"
#include "record.hpp"
#include <cassert>
#include <chrono>
#include <cstdint>

#include <iomanip>
#include <iostream>


using namespace std::chrono;


void radix_hist(const char *infile, const char *outfile)
{
    MMapFile in(infile);
    in.advise(POSIX_FADV_SEQUENTIAL);

    const std::size_t num_records = in.size() / sizeof(record);
    const auto begin_in = static_cast<record*>(in.addr());
    const auto end_in = begin_in + num_records;

    const auto t_hist_begin = high_resolution_clock::now();
    /* Compute histogram. */
    histogram_t histogram = hist_MT(begin_in, end_in, 10);
    std::cerr << histogram << std::endl;
    const auto t_hist_end = high_resolution_clock::now();
    std::cerr << "t_histogram: " << duration_cast<microseconds>(t_hist_end - t_hist_begin).count() / 1e3 << " ms\n";
    std::cerr << "histogram checksum: " << std::hex << histogram.checksum() << std::dec << std::endl;

    /* Allocate output file. */
    MMapFile out(outfile, in.size());
    out.advise(POSIX_FADV_RANDOM);

    /* Compute the partition locations. */
    const auto t_compute_begin = high_resolution_clock::now();
    std::array<record*, NUM_PARTITIONS> partitions;
    unsigned offset = 0;
    for (std::size_t i = 0; i != NUM_PARTITIONS; ++i) {
        partitions[i] = static_cast<record*>(out.addr()) + offset;
        offset += histogram[i];
    }
    assert(offset == num_records);
    const auto t_compute_end = high_resolution_clock::now();
    std::cerr << "t_compute: " << duration_cast<microseconds>(t_compute_end - t_compute_begin).count() / 1e3 << " ms\n";

    /* Sequentially scan the input data and write it to its respective partitions. */
    const auto t_write_begin = high_resolution_clock::now();
    for (auto src = begin_in; src != end_in; ++src) {
        const auto pid = src->get_radix_bits();
        record *dst = partitions[pid]++;
        memcpy(dst, src, sizeof(record));
    }
    const auto t_write_end = high_resolution_clock::now();
    std::cerr << "t_write: " << duration_cast<microseconds>(t_write_end - t_write_begin).count() / 1e3 << " ms\n";
}
