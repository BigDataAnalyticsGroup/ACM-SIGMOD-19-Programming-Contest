#!/bin/zsh

NUM_RUNS=3
BUILD_DIR=$1
FILE=$2
BIN_DIR="$BUILD_DIR/bin"

function benchmark
{
    BIN=$1
    FILE=$2
    echo "$BIN"
    if [ ! -f "$BIN" ];
    then
        >&2 echo "File does not exist: $BIN"
        return 1;
    fi

    for i in {1..$NUM_RUNS};
    do
        echo 1 | sudo tee "/proc/sys/vm/drop_caches" > /dev/null;
        time "$BIN" "$FILE";
        RET=$?
        if [ $RET -ne 0 ];
        then
            >&2 echo "Program execution failed with status code $RET";
            exit 1;
        fi
    done
    echo 1 | sudo tee "/proc/sys/vm/drop_caches" > /dev/null;
}

if [ ! -d "$BIN_DIR" ];
then
    >&2 echo "$BIN_DIR does not exist."
    exit 1
fi

if [ ! -f "$FILE" ];
then
    >&2 echo "Input file $FILE does not exist."
fi

for BIN in "$BIN_DIR/hist_"*;
do
    benchmark "$BIN" "$FILE";
done
