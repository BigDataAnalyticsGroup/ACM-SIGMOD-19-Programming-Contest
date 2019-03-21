//===== hist.hpp =======================================================================================================
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
//      This file provides algorithms for histrogram generation.
//
//======================================================================================================================

#pragma once

#include "constants.hpp"
#include <cstdio>
#include <fstream>


histogram_t hist_direct(const char *infile);

histogram_t hist_file(const char *infile);

histogram_t hist_file_seek(const char *infile);

histogram_t hist_file_custom_buffer(const char *infile);

histogram_t hist_mmap(const char *infile);

histogram_t hist_mmap_prefault(const char *infile);

histogram_t hist_mmap_MT(const char *infile, const unsigned num_threads);
