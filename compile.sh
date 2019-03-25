#!/bin/bash

BUILD_DIR="build/submission"
BUILD_TOOL="Unix Makefiles"
PROJ_DIR=$(pwd)

if [ -d "$BUILD_DIR" ];
then
    rm -r "$BUILD_DIR";
fi

mkdir -p "$BUILD_DIR";
cd "$BUILD_DIR";
cmake -G "$BUILD_TOOL" -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++ -DCMAKE_BUILD_TYPE="Release" "$PROJ_DIR"
make -j5
cd "$PROJ_DIR"
