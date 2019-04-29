#!/bin/bash

FILE=$1
DIR=$(dirname "$1")
NAME=$(basename "$1" .bin)
SUM="$DIR/$NAME.sum"

for i in {1..3};
do
    rm -f out.bin 2>/dev/null
    rm -f buckets/*.bin 2>/dev/null;
    echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null;
    sync;
    time build/release/bin/sort "$FILE" out.bin;
    sync;
    ./valsort out.bin;
    cat "$SUM";
    echo -e '\n-----\n'
done
