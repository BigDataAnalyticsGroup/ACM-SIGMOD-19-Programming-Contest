//===== utility.cpp ====================================================================================================
//
//  Author: Immanuel Haffner <haffner.immanuel@gmail.com>
//
//  Licence:
//      Copyright 2019 Immanuel Haffner
//
//      Licensed under the Apache License, Version 2.0 (the "License");
//      you may not use this file except in compliance with the License.
//      You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//      Unless required by applicable law or agreed to in writing, software
//      distributed under the License is distributed on an "AS IS" BASIS,
//      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//      See the License for the specific language governing permissions and
//      limitations under the License.
//
//  Description:
//      This file provides utility definitions.
//
//======================================================================================================================

#include "utility.hpp"

#include <cassert>
#include <err.h>
#include <fcntl.h>
#include <sched.h>
#include <thread>
#include <unistd.h>


constexpr const char * const PROC_DROP_CACHES = "/proc/sys/vm/drop_caches";

void clear_page_cache()
{

    int fd = open(PROC_DROP_CACHES, O_WRONLY);
    if (fd == -1) {
        warn("Could not open '%s' to drop caches", PROC_DROP_CACHES);
        return;
    }
    write(fd, "1", 1);
    close(fd);
}

void bind_to_cpus(std::initializer_list<unsigned> cpus)
{
    const unsigned cpu_count = std::thread::hardware_concurrency();
    assert(cpus.size() <= cpu_count);
    cpu_set_t *cpu_set = CPU_ALLOC(cpu_count);
    if (not cpu_set)
        err(EXIT_FAILURE, "Failed to allocate CPU_SET of 20 CPUs on socket 0");
    const auto size = CPU_ALLOC_SIZE(cpu_count);
    CPU_ZERO_S(size, cpu_set);
    for (auto cpu : cpus) {
        assert(cpu < cpu_count);
        CPU_SET_S(cpu, size, cpu_set);
    }
    assert(CPU_COUNT_S(size, cpu_set) == int(cpus.size()) and "allocated incorrect number of CPUs");

    /* Bind process and all children to the desired logical CPUs. */
    sched_setaffinity(0 /* this thread */, size, cpu_set);

    CPU_FREE(cpu_set);
}
