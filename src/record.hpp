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
    friend bool operator<(const key_type &first, const key_type &second) {
        {
            const uint32_t *p_first = reinterpret_cast<const uint32_t*>(first.data());
            const uint32_t *p_second = reinterpret_cast<const uint32_t*>(second.data());

            {
                const uint32_t v_first = swap_little_big_endian(p_first[0]);
                const uint32_t v_second = swap_little_big_endian(p_second[0]);
                if (v_first != v_second) return v_first < v_second;
            }

            {
                const uint32_t v_first = swap_little_big_endian(p_first[1]);
                const uint32_t v_second = swap_little_big_endian(p_second[1]);
#if 0
                std::cerr << "first is  " << first << ",\n"
                          << "second is " << second << '\n'
                          << "compare second 4 bytes:\n  "
                          << std::hex << std::setfill('0') << std::setw(8) << v_first << " vs\n  "
                          << std::hex << std::setfill('0') << std::setw(8) << v_second << " yields\n  ";
                if (v_first < v_second) std::cerr << "first less than second\n";
                else if (v_first > v_second) std::cerr << "first greater than second\n";
                else std::cerr << "first equal to second, continue\n";
#endif
                if (v_first != v_second) return v_first < v_second;
            }
        }

        {
            const uint16_t *p_first = reinterpret_cast<const uint16_t*>(first.data() + 8);
            const uint16_t *p_second = reinterpret_cast<const uint16_t*>(second.data() + 8);

            const uint16_t v_first = swap_little_big_endian(*p_first);
            const uint16_t v_second = swap_little_big_endian(*p_second);
            return v_first < v_second;
        }
    }

    /** Returns a value smaller than zero, zero, or larger than zero if the other key compares less, equal, or greater
     * then this key, respectively. */
    int compare(const key_type &other) const {
#if 0
        auto t = reinterpret_cast<const uint8_t*>(this->data());
        auto o = reinterpret_cast<const uint8_t*>(other.data());
        auto end = t + size();

        for (;;) {
            if (t == end) return 0;
            auto cmp = *t - *o;
            if (cmp) return cmp;
            ++t, ++o;
        }
#else
        return memcmp(this->data(), other.data(), size());
#endif
    }

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

    int compare(const record &other) const { return key.compare(other.key); }

    void to_ascii(std::ostream &out) const { key.to_ascii(out); payload.to_ascii(out); }

    friend std::ostream & operator<<(std::ostream &out, const record &r) { return out << r.key << "  " << r.payload; }
};
static_assert(sizeof(record) == 100, "incorrect record size");
