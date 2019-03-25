//===== hist_main.cpp ==================================================================================================
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
//      This file implements histogram generation.
//
//======================================================================================================================

#include "hist.hpp"
#include "utility.hpp"
#include <iomanip>
#include <iostream>

#ifndef METHOD
#error "You must define METHOD before compiling this source file."
#endif


histogram_t the_histogram;


int main(int argc, const char **argv)
{
    if (argc != 2) {
        std::cerr << "USAGE:\n\t" << argv[0] << " <INPUT-FILE>" << std::endl;
        std::exit(EXIT_FAILURE);
    }

    std::ios::sync_with_stdio(false);

    std::cerr << "Method " << METHOD << '\n';

    histogram_t histogram =
#if METHOD == 1
        hist_from_file_mmap(argv[1]);
#elif METHOD == 2
#ifndef THREADS
#error "To use this methods you must define THREADS"
#endif
        hist_from_file_mmap_MT(argv[1], THREADS);
#elif METHOD == 3
#ifndef THREADS
#error "To use this methods you must define THREADS"
#endif
        hist_from_file_buffered_custom_MT(argv[1], THREADS);
#elif METHOD == 4
        hist_from_file_direct(argv[1]); // does not work if file, buffer, and stride are not properly aligned
#elif METHOD == 5
        hist_from_file_unbuffered(argv[1]);
#elif METHOD == 6
        hist_from_file_buffered_default(argv[1]);
#elif METHOD == 7
        hist_from_file_buffered_custom(argv[1]);
#else
#error "Invalid METHOD"
#endif
    std::cout << "count: " << histogram.count() << "    checksum: " << std::hex << histogram.checksum() << std::endl;
}
