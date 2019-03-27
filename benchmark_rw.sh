#!/bin/zsh

NUM_RUNS=3
BUILD_DIR=$1
INPUT=$2
EXE="read_write"
BIN_DIR="$BUILD_DIR/bin"
OUTPUT="out.bin"


BIN="$BIN_DIR/$EXE"

if [ ! -f "$BIN" ];
then
    >&2 echo "Executable not found: $BIN";
    exit 1;
fi

for i in {1..$NUM_RUNS};
do
    echo 3 | sudo tee "/proc/sys/vm/drop_caches" > /dev/null;
    time "$BIN" "$INPUT" "$OUTPUT" 2>/dev/null;
    RET=$?
    if [ $RET -ne 0 ];
    then
        >&2 echo "Program execution failed with status code $RET";
        exit 1;
    fi
done
