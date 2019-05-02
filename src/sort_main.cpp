//===== main.cpp =======================================================================================================
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
//      This file provides the main method for sorting.  A preprocessor flag is used to compile as partitioning or
//      sorting binary.
//
//======================================================================================================================

#ifdef SUBMISSION
#warning "Compiling submission build"
#endif

#include "asmlib.h"
#include "ctpl.h"
#include "hwtop.hpp"
#include "mmap.hpp"
#include "record.hpp"
#include "sort.hpp"
#include "utility.hpp"
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <err.h>
#include <fcntl.h>
#include <fstream>
#include <future>
#include <iostream>
#include <sched.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>

//#define WITH_IACA
#ifdef WITH_IACA
#include <iacaMarks.h>
#else
#define IACA_START
#define IACA_END
#endif


namespace ch = std::chrono;
using namespace std::chrono_literals;

constexpr std::size_t NUM_BLOCKS_PER_SLAB = 1024;

#ifdef SUBMISSION
constexpr unsigned NUM_THREADS_READ = NUM_LOGICAL_CORES_TOTAL;
constexpr unsigned NUM_THREADS_PARTITION = NUM_LOGICAL_CORES_PER_SOCKET;
constexpr unsigned NUM_THREADS_SORT_BUCKETS = NUM_LOGICAL_CORES_TOTAL;
constexpr const char * const OUTPUT_PATH = "/output-disk";
constexpr std::size_t IN_MEMORY_BUFFER_SIZE = 28UL * 1024 * 1024 * 1024; // 26 GiB
#else
constexpr unsigned NUM_THREADS_READ = 4;
constexpr unsigned NUM_THREADS_PARTITION = 6;
constexpr unsigned NUM_THREADS_SORT_BUCKETS = 8;
constexpr const char * const OUTPUT_PATH = "./buckets";
#ifndef NDEBUG
constexpr std::size_t IN_MEMORY_BUFFER_SIZE = 1UL * 1024 * 1024 * 1024; // 1 GiB
#else
constexpr std::size_t IN_MEMORY_BUFFER_SIZE = 4UL * 1024 * 1024 * 1024; // 4 GiB
#endif
#endif

uint8_t dead_byte;


/** Information for the threads. */
struct thread_info
{
    int fd; ///< file descriptor of the file
    const char *path; ///< the path to the file to read from / write to
    std::size_t offset; ///< offset from start of the file in bytes
    std::size_t count; ///< number of bytes to read from / write to the file
    void *buffer; ///< I/O buffer to read into / write out
    std::size_t slab_size; ///< the size of a slab, the granule at which I/O is performed; a multiple of the preferred block size
};

enum class IO
{
    READ,
    WRITE
};

/* Perform reliable I/O. */
template<IO op>
void io(int fd, void *buf, std::size_t count, off_t offset)
{
    if (op == IO::READ) {
        if (posix_fadvise(fd, offset, count, POSIX_FADV_SEQUENTIAL))
            warn("Failed to advise sequential file access");
    }
    uint8_t *addr = reinterpret_cast<uint8_t*>(buf);
    while (count) {
        int num_bytes_io;
        if (op == IO::READ)
            num_bytes_io = pread(fd, addr, count, offset);
        else
            num_bytes_io = pwrite(fd, addr, count, offset);

        if (num_bytes_io == -1)
            err(EXIT_FAILURE, "Could not %s fd %d", op == IO::READ ? "read from" : "write to", fd);

        /* Discard processed pages. */
        {
            auto page_offset = offset & ~(PAGESIZE - 1); // round down to multiple of a page
            auto page_count = count & ~(PAGESIZE - 1); // round down to multiple of a page
            if (posix_fadvise(fd, page_offset, page_count, POSIX_FADV_DONTNEED))
                warn("Failed to discard bytes %lu to %lu of input", page_offset, page_offset + page_count);
        }

        count -= num_bytes_io;
        offset += num_bytes_io;
        addr += num_bytes_io;
    }
}

/* Perform reliable I/O concurrently, where the workload is distributed evenly among multiple threads. */
template<IO op>
void io_concurrent(int fd, void *buf, std::size_t count, off_t offset)
{
    uint8_t *addr = reinterpret_cast<uint8_t*>(buf);
    struct stat st;
    if (fstat(fd, &st))
        err(EXIT_FAILURE, "Could not get status of fd %d", fd);
    const std::size_t slab_size = NUM_BLOCKS_PER_SLAB * st.st_blksize;

    if (count < 2 * slab_size) {
        io<op>(fd, addr, count, offset);
    } else {
        /* Process unaligned start. */
        off_t unaligned = slab_size - (offset % slab_size);
        if (unaligned) {
            io<op>(fd, addr, unaligned, offset);
            count -= unaligned;
            addr += unaligned;
            offset += unaligned;
        }

        /* Concurrently process the rest. */
        std::array<std::thread, NUM_THREADS_READ> threads;
        const std::size_t num_slabs = std::ceil(double(count) / slab_size);
        const std::size_t num_slabs_per_thread = num_slabs / NUM_THREADS_READ;
        const std::size_t count_per_thread = num_slabs_per_thread * slab_size;
        for (unsigned tid = 0; tid != NUM_THREADS_READ; ++tid) {
            const std::size_t thread_count = tid == NUM_THREADS_READ - 1 ? count - (NUM_THREADS_READ - 1) * count_per_thread : count_per_thread;
            threads[tid] = std::thread(io<op>, fd, addr, thread_count, offset);
            addr += thread_count;
            offset += thread_count;
        }
        for (auto &t : threads)
            t.join();
    }
}

int main(int argc, const char **argv)
{
    if (argc != 3) {
        std::cerr << "USAGE:\n\t" << argv[0] << " <INPUT-FILE> <OUTPUT-FILE>" << std::endl;
        std::exit(EXIT_FAILURE);
    }

#ifdef SUBMISSION
    /* Disable synchronization with C stdio. */
    std::ios::sync_with_stdio(false);
#endif

    /* Open input file and get file stats. */
    int fd_in = open(argv[1], O_RDONLY);
    if (fd_in == -1)
        err(EXIT_FAILURE, "Could not open file '%s'", argv[1]);
    struct stat stat_in;
    if (fstat(fd_in, &stat_in))
        err(EXIT_FAILURE, "Could not get status of file '%s'", argv[1]);
    const std::size_t size_in_bytes = std::size_t(stat_in.st_size);
    const std::size_t num_records = size_in_bytes / sizeof(record);
    std::cerr << "Input file \"" << argv[1] << "\" is " << size_in_bytes << " bytes large, contains "
              << num_records << " records in blocks of size " << stat_in.st_blksize << " bytes.\n";

    /* Open output file and allocate sufficient space. */
    int fd_out = open(argv[2], O_CREAT|O_TRUNC|O_RDWR, 0644);
    if (fd_out == -1)
        err(EXIT_FAILURE, "Could not open file '%s'", argv[2]);
    if (fallocate(fd_out, /* mode = */ 0, /* offset= */ 0, /* len= */ size_in_bytes)) {
        if (errno == EOPNOTSUPP) {
            if (ftruncate(fd_out, size_in_bytes))
                err(EXIT_FAILURE, "Could not truncate file '%s' to size %lu", argv[2], size_in_bytes);
        } else {
            err(EXIT_FAILURE, "Could not allocate space for output file '%s'", argv[2]);
        }
    }

    /* MMap the output file. */
    void *output = mmap(/* addr=   */ nullptr,
                        /* length= */ size_in_bytes,
                        /* prot=   */ PROT_READ|PROT_WRITE,
                        /* flags=  */ MAP_SHARED,
                        /* fd=     */ fd_out,
                        /* offset= */ 0);
    if (output == MAP_FAILED)
        err(EXIT_FAILURE, "Could not map output file '%s' into memory", argv[2]);
    std::cerr << "Memory map the output file at virtual address " << output << ".\n";
    record *const p_out = reinterpret_cast<record*>(output);
    record *const p_end = p_out + num_records;

#ifndef NDEBUG
    /* Show histogram of the input data. */
    auto buf = reinterpret_cast<record*>(malloc(size_in_bytes));
    io_concurrent<IO::READ>(fd_in, buf, size_in_bytes, 0);
    auto hist = compute_histogram_parallel(buf, buf + num_records, 0, NUM_THREADS_READ);
    std::cerr << hist << std::endl;
    free(buf);
#endif

    /*----- Choose the algorithm based on the file size. -------------------------------------------------------------*/
    if (size_in_bytes <= IN_MEMORY_BUFFER_SIZE)
    {
        /*----- IN-MEMORY SORTING ------------------------------------------------------------------------------------*/
        std::cerr << "===== In-Memory Sorting =====\n";

        /* Lock pages of the output file on fault.  This prevents unwanted page faults during the sort. */
        syscall(__NR_mlock2, output, size_in_bytes, /* MLOCK_ONFAULT */ 1U);
        if (readahead(fd_out, 0, size_in_bytes))
            warn("Failed to queue readahead of output file");

        /* Check whether the data set is pure ASCII. */
        constexpr unsigned char FIRST_ASCII = 32u;
        constexpr unsigned char LAST_ASCII = 126u;
        bool is_pure_ASCII = true;
        /* Check whether one of the first 10 keys contains a non-ASCII byte. */
        for (std::size_t i = 0; i != 10; ++i) {
            record r;
            read(fd_in, &r, sizeof(r));
            for (uint8_t k : r.key) {
                if (k < FIRST_ASCII or k > LAST_ASCII) {
                    is_pure_ASCII = false;
                    break;
                }
            }
        }

        /* Estimate the bucket size and positions for the ASCII data set. */
        std::array<std::size_t, NUM_BUCKETS> bucket_offset_output;
        std::size_t num_records_per_bucket_avg = 0;
        if (is_pure_ASCII) {
            constexpr unsigned COUNT_ASCII_CHARS = LAST_ASCII - FIRST_ASCII + 1;
            num_records_per_bucket_avg = num_records / COUNT_ASCII_CHARS;
            std::cerr << "Assuming the data set is pure ASCII.  Predict an average bucket size of "
                      << num_records_per_bucket_avg << ".\n";
            {
                std::size_t offset = 0;
                for (std::size_t bucket_id = 0; bucket_id != NUM_BUCKETS; ++bucket_id) {
                    bucket_offset_output[bucket_id] = offset;
                    if (bucket_id >= FIRST_ASCII and bucket_id <= LAST_ASCII)
                        offset += num_records_per_bucket_avg;
                }
            }
        } else {
            num_records_per_bucket_avg = num_records / NUM_BUCKETS;
            std::cerr << "The data set contains non-ASCII values.  Predict an average bucket size of "
                      << num_records_per_bucket_avg << ".\n";
            {
                std::size_t offset = 0;
                for (std::size_t bucket_id = 0; bucket_id != NUM_BUCKETS; ++bucket_id) {
                    bucket_offset_output[bucket_id] = offset;
                    offset += num_records_per_bucket_avg;
                }
            }
        }

        /* Create atomic counters for the size of the buckets in the output file. */
        std::array<std::atomic_size_t, NUM_BUCKETS> bucket_size_output{ {0} };

        /* Create virtual overflow buckets. */
        struct bucket_t {
            std::atomic_size_t count{0};
            record *addr = nullptr;
        };
        std::array<bucket_t, NUM_BUCKETS> overflow_buckets;
        /* Assign custom, large buffer to overflow buckets. */
        for (std::size_t bucket_id = 0; bucket_id != NUM_BUCKETS; ++bucket_id)
            overflow_buckets[bucket_id].addr = new record[num_records_per_bucket_avg]; // overprovision memory for the bucket

        const auto t_begin_read = ch::high_resolution_clock::now();
        std::cerr << "Read and partition data into main memory.\n";

        auto partition = [&] (std::size_t offset, std::size_t count) {
            constexpr std::size_t NUM_RECORDS_READ = 10000;
            constexpr std::size_t NUM_RECORDS_PER_LOCAL_BUCKET = 1024;

            auto read_buffer = new record[NUM_RECORDS_READ];
            using local_bucket_t = std::array<record, NUM_RECORDS_PER_LOCAL_BUCKET>;
            auto local_buckets = new local_bucket_t[NUM_BUCKETS];
            std::array<record*, NUM_BUCKETS> heads;

            /* Initialize heads. */
            for (std::size_t bucket_id = 0; bucket_id != NUM_BUCKETS; ++bucket_id)
                heads[bucket_id] = local_buckets[bucket_id].data();

            /* This function flushes the contents of a local bucket to the output, if they fit, and falls back to the
             * overflow bucket if necessary. */
            auto flush_bucket = [&](std::size_t bucket_id) {
                assert(bucket_id < NUM_BUCKETS);
                auto &the_bucket = local_buckets[bucket_id];
                auto the_head = heads[bucket_id];

                const std::size_t num_records_to_write = the_head - the_bucket.data();
                const auto old_size_out = bucket_size_output[bucket_id].fetch_add(num_records_to_write);
                std::size_t num_records_written_to_output = 0;
                if (old_size_out < num_records_per_bucket_avg) {
                    /* Determine how many records fit into the output bucket. */
                    num_records_written_to_output = std::min(num_records_to_write, num_records_per_bucket_avg - old_size_out);
                    const std::size_t bucket_offset = bucket_offset_output[bucket_id];
                    assert(bucket_offset <= num_records);
                    /* Append these records to the output buckets. */
                    memcpy(p_out + bucket_offset + old_size_out,
                           the_bucket.data(),
                           num_records_written_to_output * sizeof(record));
                }
                if (num_records_to_write > num_records_written_to_output) {
                    /* Write the remaining records to the overflow bucket. */
                    auto &overflow_bucket = overflow_buckets[bucket_id];
                    const std::size_t num_records_left = num_records_to_write - num_records_written_to_output;
                    const auto old_size_overflow = overflow_bucket.count.fetch_add(num_records_left);
                    const auto dst = overflow_bucket.addr + old_size_overflow;
                    memcpy(dst, the_bucket.data() + num_records_written_to_output, num_records_left * sizeof(record));
#ifndef NDEBUG
                    for (auto p = dst, end = dst + num_records_left; p != end; ++p)
                        assert(p->key[0] == bucket_id);
#endif
                }
            };

            /* Read the records into the thread's read buffer, then partition them into the thread's local bucket
             * buffer.  If a local bucket runs full, flush it. */
            while (count) {
                const std::size_t to_read = std::min(count, NUM_RECORDS_READ);
                io<IO::READ>(fd_in, read_buffer, to_read * sizeof(record), offset * sizeof(record)); // fill read buffer
                count -= to_read;
                offset += to_read;

                /* Partition records in the read buffer. */
                for (auto p = read_buffer, end = read_buffer + to_read; p != end; ++p) {
                    const std::size_t bucket_id = p->key[0];
                    auto &bucket = local_buckets[bucket_id];
                    auto &head = heads[bucket_id];
                    assert(head >= bucket.begin());
                    assert(head < bucket.end());
                    *head++ = *p; // append record to thread local bucket

                    /* Check whether the local bucket is full and needs to be flushed. */
                    if (head == bucket.end()) {
                        flush_bucket(bucket_id);
                        head = bucket.data(); // reset head
                    }
                }
            }

            /* Flush the local buckets. */
            for (std::size_t bucket_id = 0; bucket_id != NUM_BUCKETS; ++bucket_id)
                flush_bucket(bucket_id);

            delete[] read_buffer;
            delete[] local_buckets;
        };

#ifdef SUBMISSION
        bind_to_cpus(SOCKET0_CPUS);
#endif
        {
            std::array<std::thread, NUM_THREADS_PARTITION> threads;
            const std::size_t count_per_thread = num_records / NUM_THREADS_PARTITION;
            std::size_t offset = 0;
            for (unsigned tid = 0; tid != NUM_THREADS_PARTITION; ++tid) {
                const std::size_t count = tid == NUM_THREADS_PARTITION - 1 ? num_records - offset : count_per_thread;
                threads[tid] = std::thread(partition, offset, count);
                offset += count;
            }
            for (unsigned tid = 0; tid != NUM_THREADS_PARTITION; ++tid)
                threads[tid].join();
        }
#ifdef SUBMISSION
        bind_to_cpus(ALL_CPUS);
#endif

#ifndef NDEBUG
        /* Validate output buckets. */
        std::size_t num_records_in_output = 0, num_records_in_overflow = 0;
        for (std::size_t bucket_id = 0; bucket_id != NUM_BUCKETS; ++bucket_id) {
            const std::size_t bucket_offset = bucket_offset_output[bucket_id];
            const std::size_t next_bucket_offset = bucket_id == NUM_BUCKETS - 1 ? num_records : bucket_offset_output[bucket_id + 1];
            /* Get the computed bucket size.  This value can have overflown and needs to be clamped. */
            std::size_t bucket_size = bucket_size_output[bucket_id].load();
            /* The bucket cannot be larger than the allocated average. */
            bucket_size = std::min(bucket_size, num_records_per_bucket_avg);
            /* The bucket cannot reach into the next bucket. */
            bucket_size = std::min(bucket_size, next_bucket_offset - bucket_offset);

            num_records_in_output += bucket_size;
            for (auto p = p_out + bucket_offset, end = p + bucket_size; p != end; ++p)
                assert(p->key[0] == bucket_id);
        }

        /* Validate overflow buckets. */
        for (std::size_t bucket_id = 0; bucket_id != NUM_BUCKETS; ++bucket_id) {
            auto &bucket = overflow_buckets[bucket_id];
            const std::size_t overflow_bucket_size = bucket.count.load();
            assert(overflow_bucket_size <= num_records_per_bucket_avg);
            num_records_in_overflow += overflow_bucket_size;
            for (auto p = bucket.addr, end = p + overflow_bucket_size; p != end; ++p)
                assert(p->key[0] == bucket_id);
        }

        std::cerr << "There are " << num_records_in_output << " records already in the output and "
                  << num_records_in_overflow << " records in the overflow buckets." << std::endl;
        assert(num_records_in_output + num_records_in_overflow == num_records);
#endif

        const auto t_begin_repair = ch::high_resolution_clock::now();

        /* Repair the output by moving in the records from the overflow buckets. */
        {
            /** Points to the next record to process. */
            record *src = p_out;
            /** Points to the output destination of the next record. */
            record *dst = src;
            std::size_t bucket_id = 0;
            for (; dst < p_end and bucket_id != NUM_BUCKETS - 1; ++bucket_id) {
                assert(bucket_id < NUM_BUCKETS - 1);
                assert(dst <= src);
                auto &of_bkt = overflow_buckets[bucket_id];
                const auto bucket_size = std::min(bucket_size_output[bucket_id].load(), num_records_per_bucket_avg);
                if (bucket_size == 0) continue; // skip empty buckets

                /* Remember where the bucket begins. */
                bucket_offset_output[bucket_id] = dst - p_out;
#ifndef NDEBUG
                /* Compute offset according to histogram. */
                {
                    std::size_t off = 0;
                    for (std::size_t i = 0; i != bucket_id; ++i)
                        off += hist[i];
                    if (off != std::size_t(dst - p_out))
                        errx(EXIT_FAILURE, "According to the histogram, bucket %lu should start at offset %lu, but we "
                                           "are at offset %lu now.", bucket_id, off, (dst - p_out));
                }
                std::cerr << "Bucket " << bucket_id << " starts at offset " << bucket_offset_output[bucket_id] << ".\n"
                          << "  ` There are " << bucket_size << " records already in the output and "
                          << of_bkt.count.load() << " records in the overflow bucket, making a total of "
                          << (bucket_size + of_bkt.count.load()) << " records.\n";
#endif

                /* Fill the gap before the bucket with the records from the overflow bucket */
                const std::size_t gap = src - dst;
                const std::size_t to_copy = std::min(gap, of_bkt.count.load());
#ifndef NDEBUG
                std::cerr << "  ` Copy " << to_copy
                          << " records from the overflow bucket into the gap before the bucket.\n";
#endif
                memcpy(dst, of_bkt.addr, to_copy * sizeof(record));
#ifndef NDEBUG
                for (auto p = dst, end = dst + to_copy; p != end; ++p)
                    assert(p->key[0] == bucket_id);
#endif
                dst += to_copy;
                assert(dst <= src);

                if (dst < src) {
                    /* If there is still a gap, we completely integrated the overflow into the output.  Move data from
                     * the end of the bucket to fill the remaining gap. */
                    const std::size_t still_gap = src - dst;
                    const std::size_t to_move = std::min(still_gap, bucket_size);
#ifndef NDEBUG
                    std::cerr << "  ` There is still a gap.  Move the bucket left by " << to_move << " records.\n";
#endif
                    memcpy(dst, src + bucket_size - to_move, to_move * sizeof(record));
#ifndef NDEBUG
                    for (auto p = dst, end = dst + bucket_size; p != end; ++p)
                        assert(p->key[0] == bucket_id);
#endif
                    dst += bucket_size; // to_move + (bucket_size - to_move)
                    src = p_out + bucket_offset_output[bucket_id + 1];
                    assert(dst < src and "we moved the current bucket at least by one record, dst must come before src");
                } else {
                    assert(dst == src);
                    /* There is still data in the overflow bucket.  Make place for that data at the end of the output
                     * bucket and move it. */
#ifndef NDEBUG
                    for (auto p = dst, end = dst + bucket_size; p != end; ++p)
                        assert(p->key[0] == bucket_id);
#endif
                    dst += bucket_size;
                    const std::size_t bucket_size_next = std::min(bucket_size_output[bucket_id + 1].load(), num_records_per_bucket_avg);
                    src = p_out + bucket_offset_output[bucket_id + 1];
                    assert(bucket_size_next == 0 or src->key[0] == bucket_id + 1);
                    assert(dst <= src and "the next bucket cannot start within the current bucket");
                    const std::size_t space_after = src - dst;
                    const std::size_t remaining = of_bkt.count.load() - to_copy;
#ifndef NDEBUG
                    std::cerr << "  ` There are " << remaining << " records remaining in the overflow bucket.  Append them to the output bucket.\n";
#endif
                    if (space_after < remaining) {
                        /* Append records to free from next bucket to its overflow bucket. */
                        const std::size_t overflow = remaining - space_after;
#ifndef NDEBUG
                        std::cerr << "    ` There is not enough space after the output bucket.  Move " << overflow
                                  << " records of the next output bucket to their overflow bucket.\n";
#endif
                        auto &of_bkt_next = overflow_buckets[bucket_id + 1];
                        memcpy(of_bkt_next.addr + of_bkt_next.count, src, overflow * sizeof(record));
                        src += overflow;
                        of_bkt_next.count += overflow;
                        bucket_size_output[bucket_id + 1] = bucket_size_next - overflow;
                        bucket_offset_output[bucket_id + 1] = src - p_out;
                    }
#ifndef NDEBUG
                    std::cerr << "  ` Copy the remaining " << remaining << " records from the overflow bufer at offset "
                              << to_copy << " to the output bucket.\n";
#endif
                    memcpy(dst, of_bkt.addr + to_copy, remaining * sizeof(record));
#ifndef NDEBUG
                    for (auto p = dst, end = dst + remaining; p != end; ++p)
                        assert(p->key[0] == bucket_id);
#endif
                    dst += remaining;
                    assert(dst <= src and "we advanced src by making space for the overflow");
                }
            }

            /* Handle the last bucket. */
            if (dst < p_end)
            {
                assert(bucket_id < NUM_BUCKETS);
                assert(dst <= src);
                auto &of_bkt = overflow_buckets[bucket_id];
                const auto bucket_size = std::min(bucket_size_output[bucket_id].load(), num_records_per_bucket_avg);

                /* Remember where the bucket begins. */
                bucket_offset_output[bucket_id] = dst - p_out;
#ifndef NDEBUG
                /* Compute offset according to histogram. */
                {
                    std::size_t off = 0;
                    for (std::size_t i = 0; i != bucket_id; ++i)
                        off += hist[i];
                    if (off != std::size_t(dst - p_out))
                        errx(EXIT_FAILURE, "According to the histogram, bucket %lu should start at offset %lu, but we "
                                           "are at offset %lu now.", bucket_id, off, (dst - p_out));
                }
                std::cerr << "Bucket " << bucket_id << " starts at offset " << bucket_offset_output[bucket_id] << ".\n"
                          << "  ` There are " << bucket_size << " records already in the output and "
                          << of_bkt.count.load() << " records in the overflow bucket, making a total of "
                          << (bucket_size + of_bkt.count.load()) << " records.\n";
#endif

                /* Fill the gap before the bucket with the records from the overflow bucket */
                const std::size_t gap = src - dst;
                const std::size_t to_copy = std::min(gap, of_bkt.count.load());
#ifndef NDEBUG
                std::cerr << "  ` Copy " << to_copy
                          << " records from the overflow bucket into the gap before the bucket.\n";
#endif
                memcpy(dst, of_bkt.addr, to_copy * sizeof(record));
#ifndef NDEBUG
                for (auto p = dst, end = dst + to_copy; p != end; ++p)
                    assert(p->key[0] == bucket_id);
#endif
                dst += to_copy;
                assert(dst == src and "For the last bucket, we must entirely fill the gap");

                /* There is still data in the overflow bucket.  Make place for that data at the end of the output
                 * bucket and move it. */
#ifndef NDEBUG
                for (auto p = dst, end = dst + bucket_size; p != end; ++p)
                    assert(p->key[0] == bucket_id);
#endif
                dst += bucket_size;
                const std::size_t remaining = of_bkt.count.load() - to_copy;
                memcpy(dst, of_bkt.addr + to_copy, remaining * sizeof(record));
#ifndef NDEBUG
                for (auto p = dst, end = dst + remaining; p != end; ++p)
                    assert(p->key[0] == bucket_id);
#endif
                dst += remaining;
            }
            assert(dst == p_end);
        }

#ifndef NDEBUG
        /* Validate the buckets in the output. */
        {
            record *p = p_out;
            for (std::size_t bucket_id = 0; bucket_id != NUM_BUCKETS; ++bucket_id) {
                const auto bucket_size = hist[bucket_id];
                for (auto end = p + bucket_size; p != end; ++p)
                    assert(p->key[0] == bucket_id);
            }
        }
#endif

        const auto t_begin_sort = ch::high_resolution_clock::now();

        /* Sort the buckets simultaneously with a worklist. */
        std::atomic_uint_fast32_t bucket_counter(0);
        auto sort_bucket = [&]() {
            unsigned bucket_id;
            while ((bucket_id = bucket_counter.fetch_add(1)) < NUM_BUCKETS) {
                const auto first = p_out + bucket_offset_output[bucket_id];
                const auto last = bucket_id == NUM_BUCKETS - 1 ? p_end : p_out + bucket_offset_output[bucket_id + 1];
                if (last > first) {
                    select_sort_algorithm(first, last, 1);
                    assert(std::is_sorted(first, last));
                }
            }
        };
        std::array<std::thread, NUM_THREADS_SORT_BUCKETS> threads;
        for (unsigned tid = 0; tid != NUM_THREADS_SORT_BUCKETS; ++tid)
            threads[tid] = std::thread(sort_bucket);
        for (unsigned tid = 0; tid != NUM_THREADS_SORT_BUCKETS; ++tid)
            threads[tid].join();
        assert(std::is_sorted(p_out, p_end));

        const auto t_begin_write = ch::high_resolution_clock::now();

        /* Write the sorted data to the output file. */
        std::cerr << "Write sorted data back to disk.\n";

        munlock(output, size_in_bytes);
#ifndef NDEBUG
        msync(output, size_in_bytes, MS_ASYNC);
        munmap(output, size_in_bytes);
        for (auto &b : overflow_buckets)
            delete[] b.addr;
#endif

        const auto t_finish = ch::high_resolution_clock::now();

        /* Report times and throughput. */
        {
            constexpr unsigned long MiB = 1024 * 1024;

            const auto d_read_s = ch::duration_cast<ch::milliseconds>(t_begin_repair - t_begin_read).count() / 1e3;
            const auto d_repair_s = ch::duration_cast<ch::milliseconds>(t_begin_sort - t_begin_repair).count() / 1e3;
            const auto d_sort_s = ch::duration_cast<ch::milliseconds>(t_begin_write - t_begin_sort).count() / 1e3;
            const auto d_write_s = ch::duration_cast<ch::milliseconds>(t_finish - t_begin_write).count() / 1e3;

            const auto throughput_read_mbs = size_in_bytes / MiB / d_read_s;
            const auto throughput_sort_mbs = size_in_bytes / MiB / d_sort_s;
            const auto throughput_write_mbs = size_in_bytes / MiB / d_write_s;

            std::cerr << "read: " << d_read_s << " s (" << throughput_read_mbs << " MiB/s)\n"
                      << "repair: " << d_repair_s << " s\n"
                      << "sort: " << d_sort_s << " s (" << throughput_sort_mbs << " MiB/s)\n"
                      << "write: " << d_write_s << " s (" << throughput_write_mbs << " MiB/s)\n";
        }
    }
    else
    {
        /*----- EXTERNAL SORTING -------------------------------------------------------------------------------------*/
        std::cerr << "===== External Sorting =====\n";

        const std::size_t num_records_to_sort = std::min(size_in_bytes, IN_MEMORY_BUFFER_SIZE) / sizeof(record);
        const std::size_t num_bytes_to_sort = num_records_to_sort * sizeof(record);
        const std::size_t num_records_to_partition = size_in_bytes <= IN_MEMORY_BUFFER_SIZE ? 0 :
            num_records - num_records_to_sort;
        const std::size_t num_bytes_to_partition = num_records_to_partition * sizeof(record);
        assert(num_records_to_partition < num_records);
        assert(num_bytes_to_sort <= size_in_bytes);
        assert(num_bytes_to_partition < size_in_bytes);
        assert(num_records_to_sort + num_records_to_partition == num_records);
        assert(num_bytes_to_sort + num_bytes_to_partition == size_in_bytes);

        const auto t_begin_read = ch::high_resolution_clock::now();

        /* Concurrently read the first part of the file. */
        std::cerr << "Read the first " << num_records_to_sort << " records ("
                  << double(num_bytes_to_sort) / (1024 * 1024) << " MiB).\n";
        void *in_memory_buffer = mmap(nullptr, num_bytes_to_sort, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS,
                                      /* fd= */ -1, 0);
        if (in_memory_buffer == MAP_FAILED)
            err(EXIT_FAILURE, "Failed to map temporary read buffer");
        io_concurrent<IO::READ>(fd_in, in_memory_buffer, num_bytes_to_sort, 0);
#ifndef NDEBUG
        std::cerr << "Allocated the in-memory buffer in range " << (void*)(in_memory_buffer) << " to "
                  << (void*)(reinterpret_cast<uint8_t*>(in_memory_buffer) + num_bytes_to_sort) << ".\n";
#endif

        ch::high_resolution_clock::time_point t_begin_sort, t_end_sort;

        /* Sort the records in-memory. */
        std::cerr << "Sort " << num_records_to_sort << " records ("
                  << double(num_bytes_to_sort) / (1024 * 1024)
                  << " MiB) in-memory in a separate thread.\n";
        histogram_t<unsigned, NUM_BUCKETS> in_memory_histogram;
        std::array<record*, NUM_BUCKETS> in_memory_buckets;
        std::thread thread_sort = std::thread([&]() {
            record * const records = reinterpret_cast<record*>(in_memory_buffer);
            t_begin_sort = ch::high_resolution_clock::now();
            american_flag_sort_parallel(records, records + num_records_to_sort, 0);
            assert(std::is_sorted(records, records + num_records_to_sort));
            t_end_sort = ch::high_resolution_clock::now();
            in_memory_histogram = compute_histogram_parallel(records, records + num_records_to_sort, 0, 10);
            in_memory_buckets = compute_buckets(records, records + num_records_to_sort, in_memory_histogram);
        });

        const auto t_begin_partition = ch::high_resolution_clock::now();

        /* Create the output files for the buckets. */
        struct bucket_t {
            std::size_t id; ///< the original id of the bucket; in [0, 255]
            FILE *file; ///< the associated stream object
            void *buffer; ///< the buffer assigned to the stream object
            std::size_t size; ///< the size of the bucket in bytes
            void *addr = nullptr; ///< the address of the memory region of the loaded bucket
            std::shared_future<void> loader; ///< the thread that loads the bucket
            std::shared_future<void> sorter; ///< the thread that sorts the bucket
        };
        std::array<bucket_t, NUM_BUCKETS> buckets;
        {
            std::ostringstream path;
            for (std::size_t bucket_id = 0; bucket_id != NUM_BUCKETS; ++bucket_id) {
                path.str("");
                path << OUTPUT_PATH << "/bucket_" << std::setfill('0') << std::setw(3) << bucket_id << ".bin";
                FILE *file = fopen(path.str().c_str(), "w+b");
                if (not file)
                    err(EXIT_FAILURE, "Could not open bucket file '%s'", path.str().c_str());

                /* Assign custom, large buffer to file stream. */
                const std::size_t buffer_size = 256 * stat_in.st_blksize;
                void *buffer = malloc(buffer_size);
                if (not buffer)
                    err(EXIT_FAILURE, "Failed to allocate I/O buffer");
                if (setvbuf(file, static_cast<char*>(buffer), _IOFBF, buffer_size))
                    err(EXIT_FAILURE, "Failed to set custom buffer for file");

                buckets[bucket_id].id = bucket_id;
                buckets[bucket_id].file = file;
                buckets[bucket_id].buffer = buffer;
            }
        }

        /* Read second part and partition. */
        if (num_records_to_partition) {
            std::cerr << "Read and partition the remaining " << num_records_to_partition << " records ("
                      << double(num_bytes_to_partition) / (1024 * 1024) << " MiB), starting at offset "
                      << num_bytes_to_sort << ".\n";

            /* Partition concurrently by evenly dividing the input between multiple partitioning threads.  Every thread
             * owns a buffer of N records for each bucket.  Read the input and append each record to the buffer of its
             * destination bucket.  If a buffer runs full, flush it to the bucket file. */
            auto partition = [fd_in, argv, &buckets] (std::size_t offset, std::size_t count) {
                constexpr std::size_t NUM_RECORDS_READ = 10000;
                constexpr std::size_t NUM_RECORDS_PER_BUFFER = 1024;

                auto read_buffer = new record[NUM_RECORDS_READ]; ///< buffer to read into from input file
                using buffer_t = std::array<record, NUM_RECORDS_PER_BUFFER>;
                auto buffers = new buffer_t[NUM_BUCKETS]; ///< write-back buffer for each bucket file
                std::array<record*, NUM_BUCKETS> heads; ///< next empty slot in each write-back buffer

                for (std::size_t bucket_id = 0; bucket_id != NUM_BUCKETS; ++bucket_id)
                    heads[bucket_id] = buffers[bucket_id].data();

                /* Read as many records at once as fit into our buffer. */
                while (count >= NUM_RECORDS_READ) {
                    const auto bytes_read = pread(fd_in, read_buffer, NUM_RECORDS_READ * sizeof(record), offset);
                    if (bytes_read != NUM_RECORDS_READ * sizeof(record)) {
                        warn("Failed to read next records from file '%s' at offset %ld", argv[1], offset);
                        return;
                    }
                    offset += bytes_read;
                    count -= NUM_RECORDS_READ;

                    /* Partition all records from the read buffer into the write-back buffers. */
                    for (auto r = read_buffer, end = read_buffer + NUM_RECORDS_READ; r != end; ++r) {
                        const unsigned bucket_id = r->key[0]; // get bucket id
                        *heads[bucket_id]++ = *r; // append to write-back buffer
                        /* If the write-back buffer is full, write it out and reset head. */
                        if (heads[bucket_id] == &buffers[bucket_id][NUM_RECORDS_PER_BUFFER]) {
                            heads[bucket_id] = buffers[bucket_id].data(); // reset head
                            auto &bucket = buckets[bucket_id];
                            auto written = fwrite(heads[bucket_id], sizeof(record), NUM_RECORDS_PER_BUFFER, bucket.file);
                            if (written != NUM_RECORDS_PER_BUFFER)
                                err(EXIT_FAILURE, "Failed to write all records of buffer to file");
                        }
                    }
                }
                assert(count < NUM_RECORDS_READ);

                /* Read remaining records at the end. */
                if (count) {
                    const auto bytes_read = pread(fd_in, read_buffer, count * sizeof(record), offset);
                    if (bytes_read != int(count * sizeof(record))) {
                        warn("Failed to read next records from file '%s' at offset %ld", argv[1], offset);
                        return;
                    }

                    for (auto r = read_buffer, end = read_buffer + count; r != end; ++r) {
                        const unsigned bucket_id = r->key[0];
                        *heads[bucket_id]++ = *r;
                        if (heads[bucket_id] == &buffers[bucket_id][NUM_RECORDS_PER_BUFFER]) {
                            heads[bucket_id] = buffers[bucket_id].data(); // reset head
                            auto &bucket = buckets[bucket_id];
                            auto written = fwrite(heads[bucket_id], sizeof(record), NUM_RECORDS_PER_BUFFER, bucket.file);
                            if (written != NUM_RECORDS_PER_BUFFER)
                                err(EXIT_FAILURE, "Failed to write all records of buffer to file");
                        }
                    }
                }

                /* Write-back the records still in the buffer. */
                for (std::size_t bucket_id = 0; bucket_id != NUM_BUCKETS; ++bucket_id) {
                    buffer_t &buffer = buffers[bucket_id];
                    auto head = heads[bucket_id];
                    if (head != buffer.data()) {
                        auto &bucket = buckets[bucket_id];
                        fwrite(buffer.data(), sizeof(record), head - buffer.data(), bucket.file);
                    }
                }

                delete[] read_buffer;
                delete[] buffers;
            };

#ifdef SUBMISSION
            bind_to_cpus(SOCKET1_CPUS);
#endif
            std::array<std::thread, NUM_THREADS_PARTITION> threads;
            const std::size_t count_per_thread = num_records_to_partition / NUM_THREADS_PARTITION;
            std::size_t offset = num_records_to_sort;
            for (unsigned tid = 0; tid != NUM_THREADS_PARTITION; ++tid) {
                const std::size_t count = tid == NUM_THREADS_PARTITION - 1 ? num_records - offset : count_per_thread;
                threads[tid] = std::thread(partition, offset * sizeof(record), count);
                offset += count;
            }
            for (unsigned tid = 0; tid != NUM_THREADS_PARTITION; ++tid)
                threads[tid].join();
#ifdef SUBMISSION
            bind_to_cpus(ALL_CPUS);
#endif
        }

        const auto t_end_partition = ch::high_resolution_clock::now();
        thread_sort.join();
        const auto t_merge_begin = ch::high_resolution_clock::now();

        /* For each partition, read it, sort it, merge with sorted records, and write out to output file. */
        std::cerr << "Merge the sorted records in memory with the buckets.\n";

        ch::high_resolution_clock::duration
            d_load_bucket_total(0),
            d_wait_for_load_bucket_total(0),
            d_sort_total(0),
            d_waiting_for_sort_total(0),
            d_merge_total(0),
            d_unmap_total(0);

        /* Flush all buckets, get the size, and compute the running sum of sizes. */
        std::array<std::size_t, NUM_BUCKETS> running_sum;
        {
            std::size_t sum = 0;
            for (std::size_t bucket_id = 0; bucket_id != NUM_BUCKETS; ++bucket_id) {
                auto &bucket = buckets[bucket_id];
                if (fflush(bucket.file))
                    err(EXIT_FAILURE, "Failed to flush bucket %lu", bucket_id);
                int fd = fileno(bucket.file);
                struct stat status;
                if (fstat(fd, &status))
                    err(EXIT_FAILURE, "Failed to get status of bucket %lu file", bucket_id);
                bucket.size = status.st_size;
                running_sum[bucket_id] = sum;
                sum += bucket.size;
            }
            assert(sum == num_bytes_to_partition and "incorrect computation of the bucket size running sum");
        }

        /* Sort buckets by file size. */
        std::sort(buckets.begin(), buckets.end(), [](const auto &first, const auto &second) {
            return first.size < second.size;
        });

        /* Create output stream for the output file */
        FILE *file_out = fdopen(fd_out, "w+");
        const std::size_t out_buffer_size = 1024 * stat_in.st_blksize;
        char *out_buffer = reinterpret_cast<char*>(malloc(out_buffer_size));
        if (setvbuf(file_out, out_buffer, _IOFBF, out_buffer_size))
            warn("Failed to set custom buffer for the output file stream");

        /* Process all buckets, in ascending order by their size.  Each bucket goes through the pipeline of three
         * stages: read - sort - merge. */
        std::size_t num_records_written_to_output = 0;
        for (std::size_t i = 0; i != NUM_BUCKETS + 2; ++i) {
            /* Load bucket i. */
            if (i < NUM_BUCKETS) {
                auto &bucket = buckets[i];
                if (bucket.size) {
                    bucket.loader = std::async(std::launch::async, [&bucket, &d_load_bucket_total]() {
                        /* Get the bucket data into memory. */
                        assert(bucket.addr == nullptr);
                        bucket.addr = malloc(bucket.size);
                        /* Lock bucket pages on fault. */
                        syscall(__NR_mlock2, bucket.addr, bucket.size, /* MLOCK_ONFAULT */ 1U);
                        if (not bucket.addr)
                            err(EXIT_FAILURE, "Failed to allocate memory for bucket file");
                        const auto t_load_bucket_begin = ch::high_resolution_clock::now();
                        io_concurrent<IO::READ>(fileno(bucket.file), bucket.addr, bucket.size, 0);
                        const auto t_load_bucket_end = ch::high_resolution_clock::now();
                        d_load_bucket_total += t_load_bucket_end - t_load_bucket_begin;
                    });
                }
            }

            /* Start sorter of bucket i-1. */
            if (i >= 1 and i <= NUM_BUCKETS) {
                auto &bucket = buckets[i - 1];
                if (bucket.size) {
                    bucket.sorter = std::async(std::launch::async, [&bucket, &d_sort_total, &d_wait_for_load_bucket_total]() {
                        /* Wait for the bucket to be loaded. */
                        const auto t_wait_for_bucket_load_begin = ch::high_resolution_clock::now();
                        bucket.loader.wait();
                        const auto t_wait_for_bucket_load_end = ch::high_resolution_clock::now();
                        d_wait_for_load_bucket_total += t_wait_for_bucket_load_end - t_wait_for_bucket_load_begin;

                        assert(bucket.addr != nullptr);
                        assert(bucket.size);
                        record *records = reinterpret_cast<record*>(bucket.addr);
                        const std::size_t num_records_in_bucket = bucket.size / sizeof(record);

                        const auto t_sort_bucket_begin = ch::high_resolution_clock::now();
                        american_flag_sort_parallel(records, records + num_records_in_bucket, 1);
                        const auto t_sort_bucket_end = ch::high_resolution_clock::now();
                        d_sort_total += t_sort_bucket_end - t_sort_bucket_begin;
                    });
                }
            }

            /* Merge bucket i-2. */
            if (i >= 2) {
                auto &bucket = buckets[i - 2];

                const std::size_t num_records_in_bucket = bucket.size / sizeof(record);
                const std::size_t num_records_in_memory = in_memory_histogram[bucket.id];
                const std::size_t num_records_merge = num_records_in_bucket + num_records_in_memory;

                const auto p_sorted_begin = in_memory_buckets[bucket.id];
                auto p_sorted = p_sorted_begin;
                const auto p_sorted_end = p_sorted + num_records_in_memory;
                assert(std::is_sorted(p_sorted_begin, p_sorted_end));

                const std::size_t bucket_offset_output = (p_sorted - reinterpret_cast<record*>(in_memory_buffer)) + // offset of the in-memory bucket
                                                         running_sum[bucket.id] / sizeof(record); // offset of the on-disk bucket
                const auto p_out_begin = p_out + bucket_offset_output;
                auto p_out = p_out_begin;
                const auto p_out_end = p_out + num_records_merge;

                /* Select whether to use the file stream or the mmap region. */
                const bool use_fstream = num_records_written_to_output < num_records_to_partition;
                num_records_written_to_output += num_records_merge;

                if (use_fstream) {
                    if (fseek(file_out, bucket_offset_output * sizeof(record), _IOFBF))
                        err(EXIT_FAILURE, "Failed to seek to the next output bucket");
                } else {
                    madvise(p_out_begin, num_records_merge * sizeof(record), MADV_SEQUENTIAL);
                    readahead(fd_out, bucket_offset_output * sizeof(record), num_records_merge * sizeof(record));
                }

                /* Wait for the sort to finish. */
                if (bucket.size) {
                    const auto t_wait_for_sort_before = ch::high_resolution_clock::now();
                    bucket.sorter.wait(); // wait for the sorter thread to finish sorting the bucket
                    const auto t_wait_for_sort_after = ch::high_resolution_clock::now();
                    d_waiting_for_sort_total += t_wait_for_sort_after - t_wait_for_sort_before;
                }

                const auto t_merge_bucket_begin = ch::high_resolution_clock::now();
                if (bucket.size) {
                    const auto p_bucket_begin = reinterpret_cast<record*>(bucket.addr);
                    auto p_bucket = p_bucket_begin;
                    const auto p_bucket_end = p_bucket + num_records_in_bucket;
                    assert(std::is_sorted(p_bucket_begin, p_bucket_end));

                    /* If the in-memory data is not yet finished, merge. */
                    assert(p_sorted != p_sorted_end and p_bucket != p_bucket_end);

                    /* Merge this bucket with the sorted data. */
                    while (p_bucket != p_bucket_end and p_sorted != p_sorted_end) {
                        IACA_START;
                        assert(p_bucket <= p_bucket_end);
                        assert(p_sorted <= p_sorted_end);
                        assert(p_out < reinterpret_cast<record*>(output) + num_records);

                        const ptrdiff_t less = *p_bucket < *p_sorted;
                        if (use_fstream)
                            fwrite_unlocked(less ? p_bucket : p_sorted, sizeof(record), 1, file_out);
                        else
                            *p_out = less ? *p_bucket : *p_sorted;
                        ++p_out;
                        p_bucket += less;
                        p_sorted += ptrdiff_t(1) - less;
                    }
                    IACA_END;
                    assert(use_fstream or std::is_sorted(p_out_begin, p_out) and "output not sorted");
                    assert(((p_bucket == p_bucket_end) or (p_sorted == p_sorted_end)) and
                            "at least one of the two inputs must be finished");

                    /* Finish the bucket, if unfinished. */
                    if (use_fstream) {
                        fwrite(p_bucket, sizeof(record), p_bucket_end - p_bucket, file_out);
                        p_bucket = p_bucket_end;
                    } else {
                        while (p_bucket != p_bucket_end)
                            *p_out++ = *p_bucket++;
                    }
                    assert(p_bucket == p_bucket_end and "consumed incorrect number of records from bucket file");
                }

                /* Finish the sorted in-memory part, if unfinished. */
                if (p_sorted != p_sorted_end) {
                    if (use_fstream) {
                        fwrite(p_sorted, sizeof(record), p_sorted_end - p_sorted, file_out);
                        p_sorted = p_sorted_end;
                    } else {
                        while (p_sorted != p_sorted_end)
                            *p_out++ = *p_sorted++;
                    }
                }
#ifndef NDEBUG
                fflush_unlocked(file_out);
#endif
                assert(p_sorted == p_sorted_end and "consumed incorrect number of records from in-memory");
                assert(use_fstream or p_out == p_out_end and "incorrect number of elements written to output");
                assert(std::is_sorted(p_out_begin, p_out) and "output not sorted");
                const auto t_merge_bucket_end = ch::high_resolution_clock::now();
                d_merge_total += t_merge_bucket_end - t_merge_bucket_begin;

                /* Release resources. */
                std::thread([=, &bucket]() {
                    constexpr uintptr_t PAGEMASK = uintptr_t(PAGESIZE) - uintptr_t(1);
                    const uintptr_t dontneed_sorted_begin = (reinterpret_cast<uintptr_t>(p_sorted_begin) + PAGEMASK) & ~PAGEMASK; // round up to page boundary
                    const uintptr_t dontneed_sorted_end = reinterpret_cast<uintptr_t>(p_sorted_end) & ~PAGEMASK; // round down to page boundary
                    const ptrdiff_t dontneed_sorted_length = dontneed_sorted_end - dontneed_sorted_begin;
                    const uintptr_t dontneed_out_begin = (reinterpret_cast<uintptr_t>(p_out_begin) + PAGEMASK) & ~PAGEMASK; // round up to page boundary
                    const uintptr_t dontneed_out_end = reinterpret_cast<uintptr_t>(p_out_end) & ~PAGEMASK; // round down to page boundary
                    const ptrdiff_t dontneed_out_length = dontneed_out_end - dontneed_out_begin;

                    if (dontneed_sorted_length)
                        munmap(reinterpret_cast<void*>(dontneed_sorted_begin), dontneed_sorted_length);
                    if (dontneed_out_length)
                        munmap(reinterpret_cast<void*>(dontneed_out_begin), dontneed_out_length);

                    if (bucket.size) {
                        munlock(bucket.addr, bucket.size);
                        free(bucket.addr);
                    }

                    if (fclose(bucket.file))
                        warn("Failed to close bucket file");
                    free(bucket.buffer);
                }).detach();

                const auto t_resource_end = ch::high_resolution_clock::now();
                d_unmap_total += t_resource_end - t_merge_bucket_end;
            }

            /* Check if the loader finished before the merger. */
            if (i < NUM_BUCKETS) {
                auto &bucket = buckets[i];
                if (bucket.size) {
                    const auto status = bucket.loader.wait_for(0ms);
                    if (status == std::future_status::ready)
                        std::cerr << "The loader of bucket " << bucket.id
                                  << " already completed and the pipeline ran stale.\n";
                }
            }
        }

        const auto t_end = ch::high_resolution_clock::now();

        /* Release resources. */
        fflush_unlocked(file_out);
        free(out_buffer);
#ifndef NDEBUG
        if (munmap(in_memory_buffer, num_bytes_to_sort))
            err(EXIT_FAILURE, "Failed to unmap the in-memory buffer");
#endif

        /* Report times and throughput. */
        {
            constexpr unsigned long MiB = 1024 * 1024;

            const auto d_read_s = ch::duration_cast<ch::milliseconds>(t_begin_partition - t_begin_read).count() / 1e3;
            const auto d_sort_s = ch::duration_cast<ch::milliseconds>(t_end_sort - t_begin_sort).count() / 1e3;
            const auto d_partition_s = ch::duration_cast<ch::milliseconds>(t_end_partition - t_begin_partition).count() / 1e3;
            const auto d_merge_s = ch::duration_cast<ch::milliseconds>(t_end - t_merge_begin).count() / 1e3;
            const auto d_total_s = ch::duration_cast<ch::milliseconds>(t_end - t_begin_read).count() / 1e3;

            const auto throughput_read_mbs = num_bytes_to_sort / MiB / d_read_s;
            const auto throughput_sort_mbs = num_bytes_to_sort / MiB / d_sort_s;
            const auto throughput_partition_mbs = num_bytes_to_partition / MiB / d_partition_s;
            const auto throughput_merge_mbs = size_in_bytes / MiB / d_merge_s;

            std::cerr << "read:      " << d_read_s << " s (" << throughput_read_mbs << " MiB/s)\n"
                      << "sort:      " << d_sort_s << " s (" << throughput_sort_mbs << " MiB/s)\n"
                      << "partition: " << d_partition_s << " s (" << throughput_partition_mbs << " MiB/s)\n"
                      << "merge:     " << d_merge_s << " s (" << throughput_merge_mbs << " MiB/s)\n"
                      << "total:     " << d_total_s << " s\n";

            std::cerr << "d_load_bucket_total: " << ch::duration_cast<ch::milliseconds>(d_load_bucket_total).count() / 1e3 << " s\n"
                      << "d_wait_for_load_total: " << ch::duration_cast<ch::milliseconds>(d_wait_for_load_bucket_total).count() / 1e3 << " s\n"
                      << "d_sort_total: " << ch::duration_cast<ch::milliseconds>(d_sort_total).count() / 1e3 << " s\n"
                      << "d_waiting_for_sort_total: " << ch::duration_cast<ch::milliseconds>(d_waiting_for_sort_total).count() / 1e3 << " s\n"
                      << "d_merge_total: " << ch::duration_cast<ch::milliseconds>(d_merge_total).count() / 1e3 << " s\n"
                      << "d_unmap_total: " << ch::duration_cast<ch::milliseconds>(d_unmap_total).count() / 1e3 << " s\n";
        }
    }

    /* Release resources. */
    close(fd_in);
    close(fd_out);

    std::exit(EXIT_SUCCESS);
}
