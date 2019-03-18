#include "sort.hpp"
#include <cstddef>
#include <cstdlib>
#include <iostream>


int main(int argc, char **argv)
{
    if (argc != 3) {
        std::cerr << "USAGE:\n\t" << argv[0] << " <INPUT-FILE> <OUTPUT-FILE>" << std::endl;
        exit(EXIT_FAILURE);
    }

    stl_sort_mmap(argv[1], argv[2]);
    exit(EXIT_SUCCESS);
}
