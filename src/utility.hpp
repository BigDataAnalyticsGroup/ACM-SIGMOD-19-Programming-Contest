//===== utility.hpp ====================================================================================================
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
//      This file provides utility definitions.
//
//======================================================================================================================

#pragma once


#include <cstdlib>
#include <initializer_list>


template<typename T>
T * allocate(std::size_t count) { return static_cast<T*>(malloc(count * sizeof(T))); }

template<typename T>
T * reallocate(T *ptr, std::size_t count) { return static_cast<T*>(realloc(ptr, count * sizeof(T))); }

template<typename T>
void deallocate(T *ptr) { free(static_cast<void*>(ptr)); }

template<typename T>
T * stack_allocate(std::size_t count) { return static_cast<T*>(alloca(count * sizeof(T))); }

void clear_page_cache();

void bind_to_cpus(std::initializer_list<unsigned> cpus);
