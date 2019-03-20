#!/bin/bash

FILES=("CMakeLists.txt" "src/" "compile.sh" "run_radix.sh" "run_sort.sh")

echo -n "Build submussion package..."
tar czf submission.tar.gz "${FILES[@]}"
echo " DONE"
