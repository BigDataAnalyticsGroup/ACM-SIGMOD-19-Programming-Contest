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


//using histogram_t = std::array<unsigned, NUM_PARTITIONS>;
struct histogram_t : public std::array<unsigned, NUM_PARTITIONS>
{
    unsigned checksum() const;

    friend std::ostream & operator<<(std::ostream &out, const histogram_t &histogram);
};

histogram_t hist(const record *begin, const record *end);
histogram_t hist_MT(const record *begin, const record *end, const unsigned num_threads);
