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


std::ostream & operator<<(std::ostream &out, const record &r)
{
    using std::setw, std::setfill;
    out << std::hex;
    for (auto k : r.key)
        out << setw(2) << setfill('0') << unsigned(k);
    out << "  ";
    for (auto p : r.payload)
        out << setw(2) << setfill('0') << unsigned(p);
    out.width(0);
    out.fill(' ');
    return out << std::dec;
}

std::ostream & operator<<(std::ostream &out, const record_key &rk)
{
    using std::setw, std::setfill;
    out << std::hex;
    for (auto k : rk.key)
        out << setw(2) << setfill('0') << unsigned(k);
    out.width(0);
    out.fill(' ');
    return out << std::dec;
}

std::array<uint8_t, 90> record_key::generate_payload_from_key() const
{
    /* Initialize random number queue from key. */
    u16 rand[10]{ {0, 0} };
#if 1
    rand[0].hi8 =
        (uint64_t(key[0]) << 56) |
        (uint64_t(key[1]) << 48) |
        (uint64_t(key[2]) << 40) |
        (uint64_t(key[3]) << 32) |
        (uint64_t(key[4]) << 24) |
        (uint64_t(key[5]) << 16) |
        (uint64_t(key[6]) << 8) |
        (uint64_t(key[7]));
    rand[0].lo8 =
        (uint64_t(key[8]) << 56) |
        (uint64_t(key[9]) << 48);
#else
    rand[0] = next_rand(skip_ahead_rand({0, 0}));
#endif

    /* Populate queue from first random number. */
    for (unsigned i = 1; i != 10; ++i)
        rand[i] = next_rand(rand[i - 1]);

    std::cerr << "queue:\n";
    for (unsigned i = 0; i != 10; ++i)
        std::cerr << "RQ[" << i << "] = 0x" << std::uppercase << std::hex
                  << std::setw(16) << std::setfill('0') << rand[i].hi8
                  << std::setw(16) << std::setfill('0') << rand[i].lo8
                  << '\n';

    std::array<uint8_t, 90> payload{ {0} };

#define GEN10(i, offset, hi, lo) { \
    u16 r = rand[i]; \
    r.hi8 ^= hi; \
    r.lo8 ^= lo; \
    (payload)[offset + 0] = (r.hi8 >> 56) & 0xFF; \
    (payload)[offset + 1] = (r.hi8 >> 48) & 0xFF; \
    (payload)[offset + 2] = (r.hi8 >> 40) & 0xFF; \
    (payload)[offset + 3] = (r.hi8 >> 32) & 0xFF; \
    (payload)[offset + 4] = (r.hi8 >> 24) & 0xFF; \
    (payload)[offset + 5] = (r.hi8 >> 16) & 0xFF; \
    (payload)[offset + 6] = (r.hi8 >>  8) & 0xFF; \
    (payload)[offset + 7] = (r.hi8 >>  0) & 0xFF; \
    (payload)[offset + 8] = (r.lo8 >> 56) & 0xFF; \
    (payload)[offset + 9] = (r.lo8 >> 48) & 0xFF; \
}

    GEN10(1,  0, 0xF0E8E4E2E1D8D4D2LL, 0xD1CC000000000000LL);
    GEN10(2, 10, 0xCAC9C6C5C3B8B4B2LL, 0xB1AC000000000000LL);
    GEN10(3, 20, 0xAAA9A6A5A39C9A99LL, 0x9695000000000000LL);
    GEN10(4, 30, 0x938E8D8B87787472LL, 0x716C000000000000LL);
    GEN10(5, 40, 0x6A696665635C5A59LL, 0x5655000000000000LL);
    GEN10(6, 50, 0x534E4D4B473C3A39LL, 0x3635000000000000LL);
    GEN10(7, 60, 0x332E2D2B271E1D1BLL, 0x170F000000000000LL);
    GEN10(8, 70, 0xC8C4C2C198949291LL, 0x8CE0000000000000LL);
    GEN10(9, 80, 0x170F332E2D2B271ELL, 0x1D1B000000000000LL);

#if 0
    rand.hi8 ^= 0xF0E8E4E2E1D8D4D2LL;   rand.lo8 ^= 0xD1CC000000000000LL;
    rand.hi8 ^= 0xCAC9C6C5C3B8B4B2LL;   rand.lo8 ^= 0xB1AC000000000000LL;
    rand.hi8 ^= 0xAAA9A6A5A39C9A99LL;   rand.lo8 ^= 0x9695000000000000LL;
    rand.hi8 ^= 0x938E8D8B87787472LL;   rand.lo8 ^= 0x716C000000000000LL;
    rand.hi8 ^= 0x6A696665635C5A59LL;   rand.lo8 ^= 0x5655000000000000LL;
    rand.hi8 ^= 0x534E4D4B473C3A39LL;   rand.lo8 ^= 0x3635000000000000LL;
    rand.hi8 ^= 0x332E2D2B271E1D1BLL;   rand.lo8 ^= 0x170F000000000000LL;
    rand.hi8 ^= 0xC8C4C2C198949291LL;   rand.lo8 ^= 0x8CE0000000000000LL;
    rand.hi8 ^= 0x170F332E2D2B271ELL;   rand.lo8 ^= 0x1D1B000000000000LL;
#endif

    return payload;
}
