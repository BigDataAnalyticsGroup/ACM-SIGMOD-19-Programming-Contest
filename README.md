# ACM SIGMOD 2019

## Programming Contest

### Specification

[Sort Benchmark FAQ](http://sortbenchmark.org/FAQ.html)

- Indy sort: 100 byte records with 10 byte keys and 90 byte payloads
- ASCII or binary, always 100 byte records

Generate data with checksum:
```plain
$ ./gensort -c <NUM_RECORDS> <FILE.bin> 2> <FILE.sum>
```
