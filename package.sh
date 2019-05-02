#!/bin/bash

FILES=("CMakeLists.txt" "src/" "asmlib/" "CTPL/" "compile.sh" "run_radix.sh" "run_sort.sh")

echo -n "Build submission package..."
tar czf submission.tar.gz "${FILES[@]}"
echo " DONE"
