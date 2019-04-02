//===== hist.cpp =======================================================================================================
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

#include "hist.hpp"


unsigned histogram_t::count() const
{
    unsigned sum = 0;
    for (auto n : *this)
        sum += n;
    return sum;
}

unsigned histogram_t::checksum() const
{
    unsigned checksum = 0;
    for (auto n : *this)
        checksum = checksum * 7 + n;
    return checksum;
}

std::ostream & operator<<(std::ostream &out, const histogram_t &histogram)
{
    out << "Histogram of " << histogram.count() << " elements:\n";
    for (std::size_t i = 0; i != histogram.size(); ++i) {
        if (histogram[i])
            out << "  " << i << ": " << histogram[i] << '\n';
    }
    return out;
}
