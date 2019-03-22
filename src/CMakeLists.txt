add_library(
    contest
    hist.cpp
    radix.cpp
    )

add_executable(hist_mmap hist_main.cpp)
target_link_libraries(hist_mmap contest ${CMAKE_THREAD_LIBS_INIT})
target_compile_definitions(hist_mmap PRIVATE METHOD=1)

add_executable(hist_mmap_MT6 hist_main.cpp)
target_link_libraries(hist_mmap_MT6 contest ${CMAKE_THREAD_LIBS_INIT})
target_compile_definitions(hist_mmap_MT6 PRIVATE METHOD=2)

add_executable(hist_mmap_MT10 hist_main.cpp)
target_link_libraries(hist_mmap_MT10 contest ${CMAKE_THREAD_LIBS_INIT})
target_compile_definitions(hist_mmap_MT10 PRIVATE METHOD=3)

add_executable(hist_direct hist_main.cpp)
target_link_libraries(hist_direct contest ${CMAKE_THREAD_LIBS_INIT})
target_compile_definitions(hist_direct PRIVATE METHOD=4)

add_executable(hist_unbuffered hist_main.cpp)
target_link_libraries(hist_unbuffered contest ${CMAKE_THREAD_LIBS_INIT})
target_compile_definitions(hist_unbuffered PRIVATE METHOD=5)

add_executable(hist_buffered_default hist_main.cpp)
target_link_libraries(hist_buffered_default contest ${CMAKE_THREAD_LIBS_INIT})
target_compile_definitions(hist_buffered_default PRIVATE METHOD=6)

add_executable(hist_buffered_custom hist_main.cpp)
target_link_libraries(hist_buffered_custom contest ${CMAKE_THREAD_LIBS_INIT})
target_compile_definitions(hist_buffered_custom PRIVATE METHOD=7)

add_executable(radix radix_main.cpp)
target_link_libraries(radix contest ${CMAKE_THREAD_LIBS_INIT})
