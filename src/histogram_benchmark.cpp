//===== histogram_benchmark.cpp ========================================================================================
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
//      This file implements benchmarks of different histogram generation approaches.
//
//======================================================================================================================

#include "benchmark.hpp"
#include "hist.hpp"
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <thread>


histogram_t the_histogram;

#define BENCHMARK(ALGO) \
    benchmark<3>(#ALGO, [&]() { the_histogram = ALGO(argv[1]); }); \
    std::cout << "checksum: " << std::hex << checksum(the_histogram) << std::dec << '\n'; \
    std::this_thread::sleep_for(2s)


using namespace std::chrono;
using namespace std::chrono_literals;


uint32_t checksum(histogram_t &histogram)
{
    uint32_t checksum = 0;
    for (auto elem : histogram)
        checksum = (checksum ^ (checksum << 16)) + elem;
    return checksum;
}


int main(int argc, const char **argv)
{
    std::ios::sync_with_stdio(false);

    if (argc != 2) {
        std::cerr << "USAGE:\n\t" << argv[0] << " <INPUT-FILE>" << std::endl;
        std::exit(EXIT_FAILURE);
    }

    BENCHMARK(hist_direct); // too slow
    BENCHMARK(hist_file);
    BENCHMARK(hist_file_seek); // too slow
    BENCHMARK(hist_file_custom_buffer);
    BENCHMARK(hist_mmap);
    BENCHMARK(hist_mmap_prefault);
}
