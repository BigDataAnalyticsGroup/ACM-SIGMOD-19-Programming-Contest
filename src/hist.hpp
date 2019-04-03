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
#include <iomanip>
#include <iostream>


/** The histogram. */
template<typename T, std::size_t N>
struct histogram_t : public std::array<T, N>
{
    /** Computes the total count of entries in the histogram. */
    unsigned count() const {
        unsigned sum = 0;
        for (auto n : *this)
            sum += n;
        return sum;
    }

    /** Computes a checksum of the histogram. */
    unsigned checksum() const {
        unsigned checksum = 0;
        for (auto n : *this)
            checksum = checksum * 7 + n;
        return checksum;
    }

    friend std::ostream & operator<<(std::ostream &out, const histogram_t &histogram) {
        using std::setw;

        out << "Histogram of " << histogram.count() << " elements:";
        for (std::size_t i = 0, count = 0; i != histogram.size(); ++i) {
            if (not histogram[i]) continue;
            if (count % 5 == 0)
                out << "\n  ";
            out << setw(3) << i << ": " << setw(5) << histogram[i] << " |    ";
            ++count;
        }
        return out << '\n';
    }
};
