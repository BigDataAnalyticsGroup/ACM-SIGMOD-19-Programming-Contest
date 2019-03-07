#include "mmap.hpp"
#include "record.hpp"
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <sys/stat.h>


constexpr std::size_t RADIX_BITS = 10LU;
constexpr std::size_t NUM_PARTITIONS = 1LU << RADIX_BITS;

void usage(std::ostream &out, const char *name)
{
    out << "USAGE:\n\t" << name << " <FILE.bin>" << std::endl;
}

int main(int argc, char **argv)
{
    if (argc != 2) {
        usage(std::cerr, argv[0]);
        exit(EXIT_FAILURE);
    }

    /* Map the file into memory. */
    const char *path = argv[1];
    MMapFile file(path);

    /* Access the data as array of struct. */
    auto data = reinterpret_cast<record*>(file.addr());
    const std::size_t count = file.size() / sizeof(*data);

    struct {
        std::ofstream file;
    } partitions[NUM_PARTITIONS];

    for (std::size_t i = 0; i != NUM_PARTITIONS; ++i) {
        std::ostringstream oss;
        oss.fill('0');
        oss << std::setw(4) << i << ".out";
        new (&partitions[i].file) std::ofstream(oss.str());
    }

    /* Partition the data. */
    for (auto p = data, end = data + count; p != end; ++p) {
        /* Extract 10 most significant bits from key. */
        const uint16_t bits = (p->key[0] << 2) | (p->key[1] >> 6);
        /* Write record to its partition. */
        partitions[bits].file.write(reinterpret_cast<char*>(p), sizeof(*p));
    }

    /* Flush before terminate. */
    for (auto &partition : partitions)
        partition.file.flush();

    exit(EXIT_SUCCESS);
}
