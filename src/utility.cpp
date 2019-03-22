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

#include <err.h>
#include <fcntl.h>
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
