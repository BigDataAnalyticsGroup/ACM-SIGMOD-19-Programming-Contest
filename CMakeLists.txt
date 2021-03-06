cmake_minimum_required(VERSION 3.0)

project("sigmod19" C CXX)

# Threads
find_package(Threads REQUIRED)

# Build flags
SET(EXECUTABLE_OUTPUT_PATH      "${PROJECT_BINARY_DIR}/bin")
SET(LIBRARY_OUTPUT_PATH         "${PROJECT_BINARY_DIR}/lib")

# Compile flags
if (APPLE)
    execute_process(COMMAND sysctl -n hw.cachelinesize
        OUTPUT_VARIABLE LEVEL1_DCACHE_LINESIZE
        OUTPUT_STRIP_TRAILING_WHITESPACE)
    execute_process(COMMAND sysctl -n hw.pagesize
        OUTPUT_VARIABLE PAGESIZE
        OUTPUT_STRIP_TRAILING_WHITESPACE)
else()
    execute_process(COMMAND getconf LEVEL1_DCACHE_LINESIZE
        OUTPUT_VARIABLE LEVEL1_DCACHE_LINESIZE
        OUTPUT_STRIP_TRAILING_WHITESPACE)
    execute_process(COMMAND getconf PAGESIZE
        OUTPUT_VARIABLE PAGESIZE
        OUTPUT_STRIP_TRAILING_WHITESPACE)
endif()
SET(CMAKE_COMPILE_FLAGS         "-W -Wall -pedantic -DLEVEL1_DCACHE_LINESIZE=${LEVEL1_DCACHE_LINESIZE} -DPAGESIZE=${PAGESIZE} -march=native -ggdb -Wno-c99-extensions -Wno-gnu-anonymous-struct -Wno-nested-anon-types -fopenmp")
SET(CMAKE_C_FLAGS               "${CMAKE_C_FLAGS} ${CMAKE_COMPILE_FLAGS}")
SET(CMAKE_CXX_FLAGS             "-std=c++17 ${CMAKE_CXX_FLAGS} ${CMAKE_COMPILE_FLAGS}")
SET(CMAKE_CXX_FLAGS_DEBUG       "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer -fno-optimize-sibling-calls -fsanitize=address")
#SET(CMAKE_CXX_FLAGS_DEBUG       "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer -fno-optimize-sibling-calls -fsanitize=memory -fsanitize-memory-track-origins=2")

# Directories
include_directories(src/ pcm/ CTPL/)
link_directories(pcm/ asmlib/)
add_subdirectory(src/)
