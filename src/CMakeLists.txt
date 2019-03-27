add_library(
    contest
    hist.cpp
    radix.cpp
    rand16.cpp
    record.cpp
    sort.cpp
    )

add_executable(read_write read_write.cpp)
target_link_libraries(read_write contest ${CMAKE_THREAD_LIBS_INIT})

#[[ too slow
add_executable(hist_mmap hist_main.cpp)
target_link_libraries(hist_mmap contest ${CMAKE_THREAD_LIBS_INIT})
target_compile_definitions(hist_mmap PRIVATE METHOD=1)
]]

add_executable(hist_mmap_MT6 hist_main.cpp)
target_link_libraries(hist_mmap_MT6 contest ${CMAKE_THREAD_LIBS_INIT})
target_compile_definitions(hist_mmap_MT6 PRIVATE METHOD=2 THREADS=6)

add_executable(hist_mmap_MT10 hist_main.cpp)
target_link_libraries(hist_mmap_MT10 contest ${CMAKE_THREAD_LIBS_INIT})
target_compile_definitions(hist_mmap_MT10 PRIVATE METHOD=2 THREADS=10)

add_executable(hist_mmap_MT16 hist_main.cpp)
target_link_libraries(hist_mmap_MT16 contest ${CMAKE_THREAD_LIBS_INIT})
target_compile_definitions(hist_mmap_MT16 PRIVATE METHOD=2 THREADS=16)

add_executable(hist_buffered_custom_MT6 hist_main.cpp)
target_link_libraries(hist_buffered_custom_MT6 contest ${CMAKE_THREAD_LIBS_INIT})
target_compile_definitions(hist_buffered_custom_MT6 PRIVATE METHOD=3 THREADS=6)

add_executable(hist_buffered_custom_MT10 hist_main.cpp)
target_link_libraries(hist_buffered_custom_MT10 contest ${CMAKE_THREAD_LIBS_INIT})
target_compile_definitions(hist_buffered_custom_MT10 PRIVATE METHOD=3 THREADS=10)

add_executable(hist_buffered_custom_MT16 hist_main.cpp)
target_link_libraries(hist_buffered_custom_MT16 contest ${CMAKE_THREAD_LIBS_INIT})
target_compile_definitions(hist_buffered_custom_MT16 PRIVATE METHOD=3 THREADS=16)

#[[ not portable; does not work if file, buffer, and stride are not properly aligned
add_executable(hist_direct hist_main.cpp)
target_link_libraries(hist_direct contest ${CMAKE_THREAD_LIBS_INIT})
target_compile_definitions(hist_direct PRIVATE METHOD=4)
]]

#[[ too slow
add_executable(hist_unbuffered hist_main.cpp)
target_link_libraries(hist_unbuffered contest ${CMAKE_THREAD_LIBS_INIT})
target_compile_definitions(hist_unbuffered PRIVATE METHOD=5)
]]

#[[ too slow
add_executable(hist_buffered_default hist_main.cpp)
target_link_libraries(hist_buffered_default contest ${CMAKE_THREAD_LIBS_INIT})
target_compile_definitions(hist_buffered_default PRIVATE METHOD=6)
]]

#[[ too slow
add_executable(hist_buffered_custom hist_main.cpp)
target_link_libraries(hist_buffered_custom contest ${CMAKE_THREAD_LIBS_INIT})
target_compile_definitions(hist_buffered_custom PRIVATE METHOD=7)
]]


add_executable(sort sort_main.cpp)
target_link_libraries(sort contest ${CMAKE_THREAD_LIBS_INIT})

add_executable(compress compress_main.cpp)
target_link_libraries(compress contest ${CMAKE_THREAD_LIBS_INIT})
