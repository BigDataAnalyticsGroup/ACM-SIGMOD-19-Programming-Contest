# ACM SIGMOD 2019

## Programming Contest

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
index to connect key with payload and makes a nice 14 byte key + index package.
