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
#include "mmap.hpp"
#include "utility.hpp"
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <thread>


histogram_t the_histogram;

#define BENCHMARK(ALGO) \
    clear_page_cache(); \
    benchmark<5>(#ALGO, [&]() { the_histogram = ALGO(begin, end); }); \
    std::cout << "checksum: " << std::hex << the_histogram.checksum() << std::dec << std::endl; \
    std::this_thread::sleep_for(2s)

#define hist_MT2(...)  hist_MT(__VA_ARGS__, 2)
#define hist_MT3(...)  hist_MT(__VA_ARGS__, 3)
#define hist_MT4(...)  hist_MT(__VA_ARGS__, 4)
#define hist_MT5(...)  hist_MT(__VA_ARGS__, 5)
#define hist_MT6(...)  hist_MT(__VA_ARGS__, 6)
#define hist_MT7(...)  hist_MT(__VA_ARGS__, 7)
#define hist_MT8(...)  hist_MT(__VA_ARGS__, 8)
#define hist_MT9(...)  hist_MT(__VA_ARGS__, 9)
#define hist_MT10(...) hist_MT(__VA_ARGS__, 10)


using namespace std::chrono;
using namespace std::chrono_literals;


int main(int argc, const char **argv)
{
    std::ios::sync_with_stdio(false);

    if (argc != 2) {
        std::cerr << "USAGE:\n\t" << argv[0] << " <INPUT-FILE>" << std::endl;
        std::exit(EXIT_FAILURE);
    }

    /* Open the input file. */
    MMapFile file(argv[1]);
    const auto data = reinterpret_cast<const record*>(file.addr());
    const std::size_t num_records = file.size() / sizeof(*data);
    const auto begin = data;
    const auto end = data + num_records;

    BENCHMARK(hist);
    BENCHMARK(hist_MT2);
    BENCHMARK(hist_MT3);
    BENCHMARK(hist_MT4);
    BENCHMARK(hist_MT5);
    BENCHMARK(hist_MT6);
    BENCHMARK(hist_MT7);
    BENCHMARK(hist_MT8);
    BENCHMARK(hist_MT9);
    BENCHMARK(hist_MT10);
}
