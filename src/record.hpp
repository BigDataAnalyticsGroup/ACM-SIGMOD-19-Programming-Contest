#pragma once

#include <cstdint>


struct __attribute__((packed)) record
{
    uint8_t key[10];
    uint8_t payload[90];
};
static_assert(sizeof(record) == 100, "incorrect record size");
