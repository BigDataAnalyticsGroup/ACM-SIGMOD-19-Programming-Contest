#include "mmap.hpp"
#include "record.hpp"
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>


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

    /* Sort all records. */
    std::sort(data, data + count, [](const record &first, const record &second) {
            return memcmp(first.key, second.key, 10) < 0;
            });

    /* Write the sorted data to stdout. */
    std::cout.write(reinterpret_cast<char*>(file.addr()), file.size());
    std::cout.flush();
    exit(EXIT_SUCCESS);
}
