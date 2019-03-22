#!/bin/zsh

BIN_DIR="build/release/bin"
FILE="resource/20G.bin"

EXE=$1
BIN="${BIN_DIR}/${EXE}"

if [ ! -f "$BIN" ];
then
    >&2 echo "File does not exist: $BIN"
    exit 1;
fi

for i in {1..5};
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
