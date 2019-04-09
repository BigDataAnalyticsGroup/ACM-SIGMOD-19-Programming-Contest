add_library(
    contest
    rand16.cpp
    record.cpp
    sort.cpp
    )

add_executable(read_write read_write.cpp)
target_link_libraries(read_write contest ${CMAKE_THREAD_LIBS_INIT})

add_executable(sort sort_main.cpp)
target_link_libraries(sort contest ${CMAKE_THREAD_LIBS_INIT} numa)

add_executable(sort_bench sorting_benchmark.cpp)
target_link_libraries(sort_bench contest ${CMAKE_THREAD_LIBS_INIT} numa)

add_executable(compress compress_main.cpp)
target_link_libraries(compress contest ${CMAKE_THREAD_LIBS_INIT})
