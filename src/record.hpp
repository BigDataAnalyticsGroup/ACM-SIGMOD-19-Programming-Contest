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

#include <array>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <iostream>


constexpr unsigned NUM_RADIX_BITS = 8;
constexpr unsigned NUM_PARTITIONS = 1 << NUM_RADIX_BITS;

struct __attribute__((packed)) record
{
    std::array<uint8_t, 10> key;
    std::array<uint8_t, 90> payload;

    /** Extracts the NUM_RADIX_BITS most significant bits from the key and places them in the lowest bits of the result.
     * All other bits are set to 0. */
    uint8_t get_radix_bits() const { return key[0]; }

    bool operator<(const record &other) const { return this->key < other.key; }
    bool operator==(const record &other) const { return this->key == other.key; }
    bool operator!=(const record &other) const { return not operator==(other); }

    friend std::ostream & operator<<(std::ostream &out, const record &r);
};
static_assert(sizeof(record) == 100, "incorrect record size");

struct record_key
{
    std::array<uint8_t, 10> key;

    bool operator<(const record &other) const { return this->key < other.key; }
    bool operator==(const record &other) const { return this->key == other.key; }
    bool operator!=(const record &other) const { return not operator==(other); }

    std::array<uint8_t, 90> generate_payload_from_key() const;

    friend std::ostream & operator<<(std::ostream &out, const record &r);
};
