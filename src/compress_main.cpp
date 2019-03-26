//===== compress_main.cpp ==============================================================================================
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
//      Try to compress input data.
//
//======================================================================================================================


#include "mmap.hpp"
#include "rand16.hpp"
#include "record.hpp"
#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>


int main(int argc, const char **argv)
{
    if (argc != 2) {
        std::cerr << "USAGE:\n\t" << argv[0] << " <INPUT-FILE>" << std::endl;
        std::exit(EXIT_FAILURE);
    }

    /* Disable synchronization with C stdio. */
    std::ios::sync_with_stdio(false);

    MMapFile in(argv[1]);
    record *data = reinterpret_cast<record*>(in.addr());
    const std::size_t num_records = in.size() / sizeof(record);

    /* Compute the initial random queue from the first record. */
    record first = data[0];
    u16 match{0, 0};
    match.hi8 =
        (uint64_t(first.key[0]) << 56) |
        (uint64_t(first.key[1]) << 48) |
        (uint64_t(first.key[2]) << 40) |
        (uint64_t(first.key[3]) << 32) |
        (uint64_t(first.key[4]) << 24) |
        (uint64_t(first.key[5]) << 16) |
        (uint64_t(first.key[6]) << 8) |
        (uint64_t(first.key[7]));
    match.lo8 =
        (uint64_t(first.key[8]) << 56) |
        (uint64_t(first.key[9]) << 48);

    std::size_t offset = 0;
    u16 seed;
    do {
        constexpr uint64_t LOMASK = 0xFFFFL << 48;
        u16 starting_rec_number{0, offset};
        seed = next_rand(skip_ahead_rand(starting_rec_number));
        if (seed.hi8 == match.hi8 and (seed.lo8 & LOMASK) == (match.lo8 & LOMASK)) goto seed_found;
        ++offset;
    } while (offset != 0);

    std::cerr << "Seed not found! Aborting...\n";
    std::exit(EXIT_FAILURE);

seed_found:
    std::cerr << "Seed found! Offset was " << offset << " (0x" << std::hex << offset << std::dec << ")\n";

    /* Populate the random queue. */
    u16 queue[10];
    queue[0] = seed;
    for (unsigned i = 1; i != 10; ++i)
        queue[i] = next_rand(queue[i-1]);

    for (unsigned i = 0; i != 10; ++i) {
        std::cerr << "RQ[" << i << "] = 0x" << std::hex
                  << std::setfill('0') << std::setw(16) << queue[i].hi8
                  << std::setfill('0') << std::setw(16) << queue[i].lo8
                  << std::dec << '\n';
    }

#if 0
    for (std::size_t i = 0; i != num_records; ++i) {
        const record &origin = data[i];
        const record_key rk{ origin.key };

        const auto payload = rk.generate_payload_from_key();
        if (payload != origin.payload) {
            std::cerr << "failed to generate payload for record at index " << i << "\n"
                      << "origin is    " << origin << "\n"
                      << "generated is " << record{ .key = rk.key, .payload = payload } << std::endl;
            std::exit(EXIT_FAILURE);
        } else {
            std::cout << i << " OK\n";
        }
    }
#endif


    std::exit(EXIT_SUCCESS);
}
