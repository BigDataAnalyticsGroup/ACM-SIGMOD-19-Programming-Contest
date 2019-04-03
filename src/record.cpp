//===== record.cpp =====================================================================================================
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
//      This file provides simple access to records.
//
//======================================================================================================================

#include "record.hpp"

#include "rand16.hpp"
#include <iomanip>


std::ostream & operator<<(std::ostream &out, const key_type &key)
{
    using std::setw, std::setfill;
    out << std::hex;
    for (auto k : key)
        out << setw(2) << setfill('0') << unsigned(k);
    return out << std::dec;
}

std::ostream & operator<<(std::ostream &out, const payload_type &payload)
{
    using std::setw, std::setfill;
    out << std::hex;
    for (auto p : payload)
        out << setw(2) << setfill('0') << unsigned(p);
    return out << std::dec;
}
