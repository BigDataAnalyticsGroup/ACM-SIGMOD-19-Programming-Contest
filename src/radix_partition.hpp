//===== radix_partition.hpp ============================================================================================
//
//  Author: Immanuel Haffner <haffner.immanuel@gmail.com>
//
//  Licence:
//      Copyright 2019 Immanuel Haffner
//
//      Licensed under the Apache License, Version 2.0 (the "License");
//      you may not use this file except in compliance with the License.
//      You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//      Unless required by applicable law or agreed to in writing, software
//      distributed under the License is distributed on an "AS IS" BASIS,
//      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//      See the License for the specific language governing permissions and
//      limitations under the License.
//
//  Description:
//      This file provides algorithms to radix partition data on disk, generated by "gensort" from the sortbenchmark.org
//      benchmarks.
//
//======================================================================================================================

#pragma once

#include "hist.hpp"


/** Partitioning algorithm from the provided example. */
void example_partition(const char *infile, const char *outfile);

void example_partition(const char *infile, const char *outfile, const histogram_t &histogram);

/** Partition by computing a histogram first and then writing records to their destinations. */
void partition_hist_mmap(const char *infile, const char *outfile, const histogram_t &histogram);
