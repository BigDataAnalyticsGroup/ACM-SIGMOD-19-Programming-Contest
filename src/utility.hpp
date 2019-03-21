#pragma once


#include <cstdlib>



template<typename T>
T * allocate(std::size_t count) { return static_cast<T*>(malloc(count * sizeof(T))); }

template<typename T>
T * reallocate(T *ptr, std::size_t count) { return static_cast<T*>(realloc(ptr, count * sizeof(T))); }

template<typename T>
void deallocate(T *ptr) { free(static_cast<void*>(ptr)); }

template<typename T>
T * stack_allocate(std::size_t count) { return static_cast<T*>(alloca(count * sizeof(T))); }
