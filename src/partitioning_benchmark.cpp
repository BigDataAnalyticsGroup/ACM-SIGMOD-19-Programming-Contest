#include "radix_partition.hpp"
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <thread>


using namespace std::chrono;
using namespace std::chrono_literals;


int main(int argc, const char **argv)
{
    if (argc != 3) {
        std::cerr << "USAGE:\n\t" << argv[0] << " <INPUT-FILE> <OUTPUT-FILE>" << std::endl;
        std::exit(EXIT_FAILURE);
    }

    {
        std::cout << "example partitioning: ";
        std::cout.flush();

        const auto t_begin = high_resolution_clock::now();
        example_partition(argv[1], argv[2]);
        const auto t_end = high_resolution_clock::now();

        std::cerr << duration_cast<milliseconds>(t_end - t_begin).count() / 1e3 << " s" << std::endl;
    }

    std::this_thread::sleep_for(5s);

    {
        std::cout << "partition_hist_mmap: ";
        std::cout.flush();

        const auto t_begin = high_resolution_clock::now();
        partition_hist_mmap(argv[1], argv[2]);
        const auto t_end = high_resolution_clock::now();

        std::cerr << duration_cast<milliseconds>(t_end - t_begin).count() / 1e3 << " s" << std::endl;
    }
}
