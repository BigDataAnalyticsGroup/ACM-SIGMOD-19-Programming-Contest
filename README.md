# ACM SIGMOD 2019

## Team "teamsic"

**Author**<br>
Immanuel Haffner <br>
*<immanuel.haffner@bigdata.uni-saarland.de>*<br>
<a href="http://bigdata.uni-saarland.de/people/haffner.php">bigdata.uni-saarland.de/people/haffner.php</a><br>
Ph.D. Student<br>
Big Data Analytics Group, Saarland Informatics Campus

**Advisor:**<br>
Prof. Dr. Jens Dittrich<br>
*<jens.dittrich@bigdata.uni-saarland.de>*<br>
<a href="http://bigdata.uni-saarland.de/people/dittrich.php">bigdata.uni-saarland.de/people/dittrich.php</a><br>
Big Data Analytics Group, Saarland Informatics Campus


## Licence

Apache License<br>
Version 2.0, January 2004<br>
[http://www.apache.org/licenses/](http://www.apache.org/licenses/)

```plain
Copyright 2019 Immanuel Haffner

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

For sake of completeness, the licence is also provided in the licence file `LICENCE.txt`.

## Poster

The poster explaining our solution - which was presented at SIGMOD'19 poster session - can be found
[here](https://github.com/BigDataAnalyticsGroup/ACM-SIGMOD-19-Programming_Contest-Poster).

## Description of the Solution

1. Distinguish between data sets that fit entirely into main memory and those that require external sorting.
2. Detect dynamically whether the data set is pure ASCII or contains non-ASCII characters.
3. Open the output file with `open()` and map it into virtual address space via `mmap()`.

### In-Memory Sorting

1. Assume a uniform distribution of the keys in their respective domain (ASCII vs. non-ASCII).
2. Partition records by their first byte (256 distinct values) into buckets.
3. Predict the bucket sizes according to domain and distribution in the virtual address space of the mapped output file.
4. Read and simultaneously partition records from the input file into the mapped output file.  Save potential overflow
   in main memory.
5. Fix overflow after partitioning.
6. Finish the 256 buckets by concurrently sorting buckets with American Flag Sort, which falls back to `std::sort` if
   the length of the sequence to sort is below a fixed threshold.
7. Need not explicitly write data to disk, as the kernel will take care of the dirty mapped pages ;)

### External Sorting

1. Read the first 28 GiB into main meory.
2. Start sorting the in-memory data in a separate thread.
3. Read remaining data from input file and simultaneously partition it by write records to 256 bucket files on the
   output disk.
4. Wait for sorting and partitioning to complete.
5. Build a priority queue of buckets, sorted by the bucket size in ascending order.
6. Merge the buckets on disk with the in-memory data, bucket by bucket, according to the priority queue.  This happens
   in a decoupled pipeline, where bucket i+2 is read while bucket i+1 is sorted while bucket i is merged and written to
   the output file.
7. For the remaining 28GiB of the file, don't explicitly write data to the file but instead write it the the mapped
   memory.  Lock the pages in memory on fault.  This trades write performance (~477MiB/s) for read performance
   (~515MiB/s).

## Third-Party Libraries

### C++ Thread Pool Library (CTPL)

"Modern and efficient C++ Thread Pool Library" available on [Github](https://github.com/vit-vit/CTPL)

**Licence:** Apache v2

(Internally, one of the two implementations uses
[boost's lock free queue](https://www.boost.org/doc/libs/1_66_0/doc/html/lockfree/reference.html#header.boost.lockfree.queue_hpp).)

### Processor Counter Monitor (PCM)

"Processor Counter Monitor (PCM) is an application programming interface (API) and a set of tools based on the API to
monitor performance and energy metrics of Intel® Core™, Xeon®, Atom™ and Xeon Phi™ processors."

Available on [Github](https://github.com/opcm/pcm)

**Licence:** custom open source licence (see Github repository)

### Agner Fog's Subroutine Library

The `asmlib` subroutine library by [Agner Fog](https://www.agner.org/optimize/#asmlib).

**Licence:** GNU GPL v3

## Task Description

### Specification

[Sort Benchmark FAQ](http://sortbenchmark.org/FAQ.html)

- Indy sort: 100 byte records with 10 bytes key and 90 bytes payload
- ASCII or binary, always 100 byte records

Generate data with checksum:
```plain
$ ./gensort -c <NUM_RECORDS> <FILE.bin> 2> <FILE.sum>
```


## Ideas, Facts, and Lessons Learned

### File I/O

[A journey on fast reading and writing in
Linux.](https://blog.plenz.com/2014-04/so-you-want-to-write-to-a-file-real-fast.html)

#### Reading

- To read a file sequentially, use buffered I/O from *glibc* with a custom, large buffer (64 KiB?)
    - open file with `fopen(path, "rb")`
    - set buffer with `setvbuf()` and `mode` to `_IOFBF` (fully buffered)
    - write the file via `fwrite()` (granularity doesn't really matter here, since it is buffered)

- On SSDs with concurrent operations (multiple lanes), concurrently reading is necessary to achieve maximum throughput
    - get the file stats (size, preferred block size, etc) with `fstat()`
    - divide the file into as many consecutive chunks as you have reader threads
    - make sure to align the chunks to the preferred block size (or multiples thereof, called *slabs*)
    - each thread reads its chunk, a slab at a time
        - use `pread()` to read the file at a particular offset; from the man page:<br/>
        *"The pread() and pwrite() system calls are especially useful in
        multithreaded applications.  They allow multiple threads to  perform  I/O on
        the same file descriptor without being affected by changes to the file offset
        by other threads."*

#### Writing

- To write file sequentially without thrashing the page cache, follow [Linus' advice from the Linux Kernel Developer
  mailing list](http://lkml.iu.edu/hypermail/linux/kernel/1005.2/01845.html)
    - write file in large chunks (he uses 8 MiB)
    - after issuing write-out of chunk *n*, request to sync write-out of chunk *n* and wait for sync of chunk *n-1* to
      disk using `sync_file_range()` (blocks until sync is done)
    - Linus further explains and recommends the use of `posix_fadvise()` to mark pages in the cache that were just synced
      for removal; this relaxes cache pressure; **NOTE** that `posix_fadvise()` gives a hint to the OS about the *current*
      state and not the *future*
    - briefly summarized on
      [StackOverflow](https://stackoverflow.com/questions/3755765/what-posix-fadvise-args-for-sequential-file-write)

- If a file must be *written randomly*, it is worthwhile to issue the writes concurrently.

- Concurrently writing an entire file (that could very well be written sequentially) is not beneficial.

- Final write of the result output file can be delayed. (This really feels like **cheating**.)
    - Create fresh output file with `open(path, O_CREAT|O_TRUNC|O_RDWR, 0644)`
    - Allocate space for the file:
      ```
      if (fallocate(fd, mode, offset, len) == EOPNOTSUPP)
          ftruncate(fd, len);
      ```
        - prefer `fallocate()`, but if not supported fall back to `ftruncate()`
        - avoid `posix_fallocate()`; the man page says:<br/>
            *"If  the  underlying filesystem does not support fallocate(2), then the operation is emulated with the
            following caveats:<br/>
            \* The emulation is inefficient."*
    - `mmap()` the output file with `PROT_READ|PROT_WRITE` and `MAP_SHARED`
        - the `prot` settings allow us to read and write to the mapped memory region
        - the flag `MAP_SHARED` tells the OS that changes should be carried through to the underlying file
    - To *"write"* the file eventually, just issue `msync(MS_ASYNC)`
        - quoting the man page:<br/>
          *"Since Linux 2.6.19, MS_ASYNC is in fact a no-op, since the kernel properly tracks dirty pages and flushes
          them to storage as necessary."*
    - This strategy leaves the kernel in duty to properly write the `mmap()`'d file from the page cache to disk.  The
      program is free to terminate (or crash), the kernel will write the most recent state of the memory to the file on
      disk.

#### Asynchronous I/O

Linux implements the [*POSIX asynchronous I/O* (aio)](https://linux.die.net/man/7/aio)


### Sorting

#### Getting Started

- To satisfy functional completeness of the test harness, for testing, and for simplicity, use STL's `std::sort` on an
  array of records.
    - horribly slow
- To exploit the multiple CPU cores with low effort, [use GNU's parallel
  mode](https://gcc.gnu.org/onlinedocs/libstdc++/manual/parallel_mode_using.html)
    - fire & forget with `__gnu_parallel::sort()`
    - exhausts all cores, but still slow

#### Splitting Records into Keys and Values

Records are 100 bytes long, composed of 10 bytes key and 90 bytes payload.  Moving the payload together with the key
induces heavy copying during the sort.  We can reduce the copy overhead by creating an index into the records array and
sorting the index (sometimes called *tag sort*).

The largest file is 100 GiB, approx. 100 * 1024³ bytes or 1024³ = 2³30 records.  Therefore, a 4 byte integer suffices as
index to connect key with payload and makes a nice 14 byte key + index package.  To index the entire 100 GiB file one
needs approximately 100 GiB * (14 B / 100 B) = 14 GiB, which nicely fits into main memory and still leaves approx. 16
GiB memory for other usage.

Such a tag sort requires reconstruction of the tuples.  Does the index creation and the sorting of the index outperform
a regular sort?  How much memory is required and is it even feasible in our setting?  For the tag sort, we must store
the payloads (90 B), the key-index pairs (14 B), and the reconstructed output data (100 B).  This means, we need more
than 2x the input data as memory.

#### Exploiting Sorted Sequences in the Input

Some popular and practically efficient sorting algorithms (like Timsort) exploit existent sorted sequences, or runs, in
the input.  Since in our scenario data is generated randomly (uniformly or skewed), it is very unlikely to expect long
runs.  Therefore, it is not in our interest to exploit existent runs.

#### Choosing the right Category of Sorting Algorithm

Radix sort algorithms are generally used to sort long objects, such as strings, where comparisons are expensive.  In our
scenario, the key is a 10 byte sequence.

#### Overlapping Reading with Sorting

To reduce end-to-end times of the application, we should reduce the latency between the first read and sorting, and
exploit the computational power of the system.  When only reading data, the CPU sits mostly idle, waiting for the I/O of
the drive.  It seems promising to interleave reading with sorting somehow.

##### Run Generation using Tournament Replacement Selection Sort (Fully Interleaved)

We can fully interleave reading with sorting by performing a tournament sort with every next record read.  The locations
of the runs must then be saved in some auxiliary data structure and a (recurrent and multi-way) merge must be performed
to get the fully-sorted list.

##### Run Generation using Insertion Sort (Fully Interleaved)

We can interleave the reading with sorting by inserting every read record in sorted order into a sequence.  These
sequences can have an upper limit for the length, when insertion sort becomes sub par, and a new sequence is started.
This way, sequences have the same fixed length.

##### Blocked Reading and Sorting

Instead of fully interleaving the read and the sort, we can perform reading and sorting block-wise and overlap the two
operations.  After reading a block of records, a sort for this block is initiated while the next block is read from the
input.  Eventually, we have a sequence of fixed-length blocks of sorted records.  Again, we must merge them eventually.

#### Overlapping Merging with Writing

When wiritng data out to disk, we can merge multiple sorted sequences that are kept in memory into one.  The write
performance is significantly less than the throughput at which we can merge the sequences.

For input sizes that fit the main memory entirely, it is likely to be better to fully sort them and delay and delegate
the write out to the kernel.

#### Sorting Algorithms

[Some thoughts of boost on radix
sort](https://www.boost.org/doc/libs/1_69_0/libs/sort/doc/html/sort/single_thread/spreadsort/sort_hpp/rationale/hybrid_radix.html)

##### American Flag Sort

[American Flag Sort](https://en.wikipedia.org/wiki/American_flag_sort) is an in-place variant of MSD radix sort.  It
works by iteratively distributing items into buckets.  A histogram generation pass is used to compute the locations of
the buckets, such that items can directly be moved to their correct position.

American Flag Sort follows the divide-and-conquer strategy, and hence exposes a high degree of task parallelism.  This
makes it very suitable for exploiting the many cores of the target system.

##### LSD Radix Sort

A [Least Significant Radix Sort](https://en.wikipedia.org/wiki/Radix_sort#Least_significant_digit_radix_sorts) sorts
items by iteratively *stable* sorting the items by a single digit, starting with the least significant digit and
advancing until the items have been sorted by the most significant digit.  Because the sort by digit is stable, sorting
by digit *n* preserves the order of all items that are equal w.r.t. digits *0...n-1*.  Because every step of the LSD
radix sort is stable, the entire sort is stable, too.

LSD Radix Sort performs independently of the data distribution.  It's performance is solely determined by data set size
and key length.  Since there is no best- or worst-case for LSD Radix Sort, it fails to exploit significant structure in
the data.  However, in our scenario, data is randomly generated with either uniform or skewed distribution.  It is
unlikely, that there is much structure to exploit.  On the other side, the 10 byte, fixed-length keys and the large data
set size make LSD an interesting contestant.  The fact, that LSD radix sort works out-of-place and requires additional
memory linear in the size of the input, could be a dealbreaker.
