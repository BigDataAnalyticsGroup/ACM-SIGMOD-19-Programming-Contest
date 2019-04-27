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
#include <functional>
#include <iomanip>
#include <iostream>


namespace {

constexpr uint32_t FIRST_BYTE  = 0x000000FF;
constexpr uint32_t SECOND_BYTE = 0x0000FF00;
constexpr uint32_t THIRD_BYTE  = 0x00FF0000;
constexpr uint32_t FOURTH_BYTE = 0xFF000000;

inline uint16_t swap_little_big_endian(uint16_t v)
{
    return ((v & FIRST_BYTE) << 8) | ((v & SECOND_BYTE) >> 8);
}

inline uint32_t swap_little_big_endian(uint32_t v)
{
    return ((v & FIRST_BYTE)  << 24) |
           ((v & SECOND_BYTE) <<  8) |
           ((v & THIRD_BYTE)  >>  8) |
           ((v & FOURTH_BYTE) >> 24);
}

}

struct __attribute__((packed)) key_type : std::array<uint8_t, 10>
{
    private:
    template<class Compare>
    static bool compare(const key_type &first, const key_type &second, const Compare &cmp) {
        /* Load first 8 bytes. */
        const uint32_t *p_first = reinterpret_cast<const uint32_t*>(first.data());
        const uint32_t *p_second = reinterpret_cast<const uint32_t*>(second.data());
        const uint64_t v0_first = uint64_t(swap_little_big_endian(p_first[0])) << 32UL |
                                 uint64_t(swap_little_big_endian(p_first[1]));
        const uint64_t v0_second = uint64_t(swap_little_big_endian(p_second[0])) << 32UL |
                                 uint64_t(swap_little_big_endian(p_second[1]));

        /* Load last 2 bytes. */
        const uint16_t v1_first = swap_little_big_endian(*reinterpret_cast<const uint16_t*>(first.data() + 8));
        const uint16_t v1_second = swap_little_big_endian(*reinterpret_cast<const uint16_t*>(second.data() + 8));

        /* Compare. */
        const bool take0 = v0_first != v0_second;
        const bool cmp0 = cmp(v0_first, v0_second);
        const bool cmp1 = cmp(v1_first, v1_second);

        return (take0 and cmp0) or (not take0 and cmp1);
    }

    public:
    friend bool operator<(const key_type &first, const key_type &second) {
        return compare(first, second, std::less<uint64_t>{});
    }
    friend bool operator>(const key_type &first, const key_type &second) {
        return compare(first, second, std::greater<uint64_t>{});
    }
    friend bool operator!=(const key_type &first, const key_type &second) {
        return compare(first, second, std::not_equal_to<uint64_t>{});
    }
    friend bool operator<=(const key_type &first, const key_type &second) { return not (first > second); }
    friend bool operator>=(const key_type &first, const key_type &second) { return not (first < second); }
    friend bool operator==(const key_type &first, const key_type &second) { return not (first != second); }


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
