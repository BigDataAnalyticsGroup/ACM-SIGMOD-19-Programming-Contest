//===== hwtop.cpp ======================================================================================================
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
//      Defines constants that express the hardware topology of the evaluation system.
//
//======================================================================================================================


#pragma once

#include <initializer_list>


constexpr std::size_t NUM_SOCKETS = 2;
constexpr unsigned NUM_HW_CORES_PER_SOCKET = 10;
constexpr unsigned NUM_THREADS_PER_HW_CORE = 2;
constexpr unsigned NUM_LOGICAL_CORES_PER_SOCKET = NUM_HW_CORES_PER_SOCKET * NUM_THREADS_PER_HW_CORE;
constexpr unsigned NUM_LOGICAL_CORES_TOTAL = NUM_LOGICAL_CORES_PER_SOCKET * NUM_SOCKETS;

constexpr std::initializer_list<unsigned> ALL_CPUS = {
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
    10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
    20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
    30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
};

constexpr std::initializer_list<unsigned> SOCKET0_CPUS = {
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
    20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
};

constexpr std::initializer_list<unsigned> SOCKET1_CPUS = {
    10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
    30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
};
