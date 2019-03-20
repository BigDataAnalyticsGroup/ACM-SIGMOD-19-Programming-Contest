# Benchmark executables
add_executable(
    histogram_benchmark
    histogram_benchmark.cpp
    hist.cpp
    )

add_executable(
    partitioning_benchmark
    partitioning_benchmark.cpp
    radix_partition.cpp
    )

add_executable(
    sorting_benchmark
    sorting_benchmark.cpp
    sort.cpp
    )

# Submission executables
add_executable(
    sort
    main.cpp
    radix_partition.cpp
    sort.cpp
    )
target_compile_definitions(sort PRIVATE METHOD=stl_sort_mmap_direct)

add_executable(
    radix
    main.cpp
    radix_partition.cpp
    sort.cpp
    )
target_compile_definitions(radix PRIVATE METHOD=partition_hist_mmap)
