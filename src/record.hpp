//===== record.hpp =====================================================================================================
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

#pragma once

#include <cassert>
#include <cstdint>
#include <cstring>


struct __attribute__((packed)) record
{
    uint8_t key[10];
    uint8_t payload[90];

    /** Extracts the 10 most significant bits from the key and places them in the 10 lowest bits of the result.  All
     * other bits are set to 0. */
    uint16_t get_radix_bits() const
    {
        uint16_t radix = (key[0] << 2) | (key[1] >> 6);
        assert(radix < 1024 and "radix out of range");
        return radix;
    }

    bool operator<(const record &other) const { return memcmp(this->key, other.key, 10) < 0; }
    bool operator==(const record &other) const { return memcmp(this->key, other.key, 10) == 0; }
    bool operator!=(const record &other) const { return not operator==(other); }
};
static_assert(sizeof(record) == 100, "incorrect record size");
