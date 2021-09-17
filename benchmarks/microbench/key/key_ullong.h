#pragma once

#include <limits.h> //> For INT_MAX

typedef unsigned long long map_key_t;
#define MAX_KEY ULLONG_MAX
#define MIN_KEY 0
#define KEY_PRINT(k, PREFIX, POSTFIX) printf("%s%llu%s", (PREFIX), (k), (POSTFIX))

#define KEY_CMP(k1, k2) ((k1) - (k2))
#define KEY_COPY(dst, src) ((dst) = (src))
#define KEY_GET(k, someuint64) ((k) = (someuint64))
#define KEY_ADD(dst, k1, k2) ((dst) = (k1) + (k2))
