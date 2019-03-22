//===== mmap.hpp =======================================================================================================
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
//      This file provides a simple interface to memory mapped files.
//
//======================================================================================================================

#pragma once

#include <algorithm>
#include <fcntl.h>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>


struct MMapFile
{
    MMapFile(const char *filename, int flags = O_RDONLY, bool populate = false) : filename(filename) {
        fd_ = open(filename, flags);
        if (fd_ == -1)
            throw std::runtime_error("failed to open file");

        struct stat statbuf;
        if (fstat(fd_, &statbuf) != 0)
            throw std::runtime_error("failed to stat file");
        size_ = statbuf.st_size;

        if (populate)
            addr_ = mmap(nullptr, size_, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_POPULATE, fd_, 0);
        else
            addr_ = mmap(nullptr, size_, PROT_READ|PROT_WRITE, MAP_PRIVATE, fd_, 0);
        if (addr_ == MAP_FAILED)
            throw std::runtime_error("failed to mmap file");
    }

    ~MMapFile() {
        if (munmap(addr_, size_) != 0)
            std::cerr << "Failed to unmap memory mapped file" << std::endl;
        if (close(fd_) != 0)
            std::cerr << "Failed to close file" << std::endl;
    }

    /** Return the address to the beginning of the mapped file. */
    void * addr() const { return addr_; }
    /** Return the size in bytes of the mapped file. */
    std::size_t size() const { return size_; }

    /* Announce an intention to access file data in a specific pattern. */
    void advise(int advice) { posix_fadvise(fd_, 0, size_, advice); }

    const char * const filename;
    private:
    void *addr_;
    std::size_t size_;
    int fd_;
};
