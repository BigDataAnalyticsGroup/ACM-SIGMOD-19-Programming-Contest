#!/bin/bash

# Summarize all non-empty partitions
for P in *.out;
do
    BYTES=$(wc -c "$P" | cut --delimiter=' ' --fields='1')
    if [ 0 -ne "$BYTES" ];
    then
        >&2 echo "Validate $P"
        NAME="$(basename $P .out).sum"
        ./valsort -o "$NAME" "$P" 2>/dev/null
    fi
done

cat *.sum > all.sum
./valsort -s all.sum
rm *.out *.sum
