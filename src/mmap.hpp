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
    MMapFile(const char *filename) : filename(filename) {
        fd_ = open(filename, O_RDONLY);
        if (fd_ == -1)
            throw std::runtime_error("failed to open file");

        struct stat statbuf;
        if (fstat(fd_, &statbuf) != 0)
            throw std::runtime_error("failed to stat file");
        size_ = statbuf.st_size;

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

    const char * const filename;
    private:
    void *addr_;
    std::size_t size_;
    int fd_;
};
