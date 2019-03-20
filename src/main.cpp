//===== main.cpp =======================================================================================================
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
//      This file provides the main method for the binaries for partitioning and sorting.  A preprocessor flag is used
//      to compile as partitioning or sorting binary.
//
//======================================================================================================================


#ifndef METHOD
#error "Define METHOD before compiling."
#endif


#include "radix_partition.hpp"
#include "sort.hpp"
#include <cstddef>
#include <cstdlib>
#include <iostream>


int main(int argc, const char **argv)
{
    if (argc != 3) {
        std::cerr << "USAGE:\n\t" << argv[0] << " <INPUT-FILE> <OUTPUT-FILE>" << std::endl;
        std::exit(EXIT_FAILURE);
    }

    std::ios::sync_with_stdio(false);

    METHOD(argv[1], argv[2]);

    std::exit(EXIT_SUCCESS);
}
