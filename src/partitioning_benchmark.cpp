#include "benchmark.hpp"
#include "hist.hpp"
#include "radix_partition.hpp"
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <thread>


#define BENCHMARK(ALGO) \
    benchmark(#ALGO, [=]() { ALGO(argv[1], argv[2]); }); \
    std::this_thread::sleep_for(2s)

#define BENCHMARK_WITH_HISTOGRAM(ALGO) \
    benchmark(#ALGO, [=]() { ALGO(argv[1], argv[2], histogram); }); \
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

    /* Compute the histogram used for partitioning. */
    histogram_t histogram = hist_mmap_MT(argv[1], 7);

    //BENCHMARK(example_partition);
    BENCHMARK_WITH_HISTOGRAM(example_partition);
    BENCHMARK_WITH_HISTOGRAM(partition_hist_mmap);
}
