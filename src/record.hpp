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


struct __attribute__((packed)) key_type : std::array<uint8_t, 10>
{
    void to_ascii(std::ostream &out) const { for (auto c : *this) out << char(c); }

    friend std::ostream & operator<<(std::ostream &out, const key_type &key);
};

struct __attribute__((packed)) payload_type : std::array<uint8_t, 90>
{
    bool operator<(const payload_type &) = delete;
    bool operator>(const payload_type &) = delete;
    bool operator<=(const payload_type &) = delete;
    bool operator>=(const payload_type &) = delete;
    bool operator==(const payload_type &) = delete;
    bool operator!=(const payload_type &) = delete;

    void to_ascii(std::ostream &out) const { for (auto c : *this) out << char(c); }

    friend std::ostream & operator<<(std::ostream &out, const payload_type &payload);
};

struct __attribute__((packed)) record
{
    key_type key;
    payload_type payload;

    bool operator<(const record &other) const { return this->key < other.key; }
    bool operator>(const record &other) const { return this->key > other.key; }
    bool operator<=(const record &other) const { return this->key <= other.key; }
    bool operator>=(const record &other) const { return this->key >= other.key; }
    bool operator==(const record &other) const { return this->key == other.key; }
    bool operator!=(const record &other) const { return this->key != other.key; }

    void to_ascii(std::ostream &out) const { key.to_ascii(out); payload.to_ascii(out); }

    friend std::ostream & operator<<(std::ostream &out, const record &r) { return out << r.key << "  " << r.payload; }
};
static_assert(sizeof(record) == 100, "incorrect record size");
