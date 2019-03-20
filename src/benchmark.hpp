//===== benchmark.hpp ==================================================================================================
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
//      This file provides routines for benchmarking with standardized output formats.
//
//======================================================================================================================

#pragma once


#include <algorithm>
#include <chrono>
#include <functional>
#include <iostream>


template<typename Rep, typename Period>
void print_time(std::chrono::duration<Rep, Period> d)
{
    using std::chrono::duration_cast;

    auto n = duration_cast<std::chrono::nanoseconds>(d).count();

    if (n <= 1000) {
        std::cout << n << " ns";
        return;
    }

    const char *format = "µs";
    if (n >= 1e6) {
        n = duration_cast<std::chrono::microseconds>(d).count();
        format = "ms";
    }
    if (n >= 1e6) {
        n = duration_cast<std::chrono::milliseconds>(d).count();
        format = "s";
    }
    std::cout << n / 1000. << ' '  << format;
}

template<unsigned NUM_RUNS = 5>
void benchmark(const char *name, std::function<void(void)> fn)
{
    using namespace std::chrono;
    using dur_t = high_resolution_clock::duration;
    std::array<dur_t, NUM_RUNS> times;

    std::cout << name << ':';

    /* Perform all the runs. */
    for (auto &d : times) {
        const auto t_begin = std::chrono::high_resolution_clock::now();
        fn();
        const auto t_end = std::chrono::high_resolution_clock::now();
        d = t_end - t_begin;
        std::cout << ' ';
        print_time(d);
        std::cout.flush();
    }

    /* Compute median running time. */
    std::sort(times.begin(), times.end());
    dur_t median;
    if (NUM_RUNS % 2)
        median = times[NUM_RUNS / 2];
    else
        median = (times[NUM_RUNS / 2 - 1] + times[NUM_RUNS / 2]) / 2;
    std::cout << " Median: ";
    print_time(median);
    std::cout << std::endl;
}
