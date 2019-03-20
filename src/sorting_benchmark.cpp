#include "benchmark.hpp"
#include "sort.hpp"
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <thread>


#define BENCHMARK(ALGO) \
    benchmark(#ALGO, [=]() { ALGO(argv[1], argv[2]); }); \
    std::this_thread::sleep_for(2s)


using namespace std::chrono;
using namespace std::chrono_literals;


int main(int argc, const char **argv)
{
    std::ios::sync_with_stdio(false);

    if (argc != 3) {
        std::cerr << "USAGE:\n\t" << argv[0] << " <INPUT-FILE> <OUTPUT-FILE>" << std::endl;
        std::exit(EXIT_FAILURE);
    }

    BENCHMARK(example_sort);
    BENCHMARK(stl_sort_mmap_direct);
    BENCHMARK(stl_sort_mmap_file);
    BENCHMARK(stl_sort_mmap_stream);
    BENCHMARK(stl_sort_mmap_file_custom_buffer);
    BENCHMARK(stl_sort_mmap_stream_custom_buffer);
}
