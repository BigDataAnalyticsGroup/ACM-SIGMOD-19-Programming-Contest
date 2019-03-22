# Benchmark executables
add_executable(
    histogram_benchmark
    histogram_benchmark.cpp
    hist.cpp
    )
target_link_libraries(histogram_benchmark ${CMAKE_THREAD_LIBS_INIT})

#add_executable(
    #sorting_benchmark
    #sorting_benchmark.cpp
    #sort.cpp
    #)
#target_link_libraries(sorting_benchmark ${CMAKE_THREAD_LIBS_INIT})

# Submission executables
add_executable(
    radix
    radix_main.cpp
    hist.cpp
    radix.cpp
    )
target_link_libraries(radix ${CMAKE_THREAD_LIBS_INIT})

#add_executable(
    #sort
    #main.cpp
    #radix_partition.cpp
    #sort.cpp
    #)
#target_compile_definitions(sort PRIVATE METHOD=stl_sort_mmap_direct)
#target_link_libraries(sort ${CMAKE_THREAD_LIBS_INIT})
