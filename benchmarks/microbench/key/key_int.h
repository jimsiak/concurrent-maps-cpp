#pragma once

#include <limits.h> //> For INT_MAX

typedef int map_key_t;
#define MAX_KEY INT_MAX
#define MIN_KEY -1
#define KEY_PRINT(k, PREFIX, POSTFIX) printf("%s%d%s", (PREFIX), (k), (POSTFIX))

#define KEY_CMP(k1, k2) ((k1) - (k2))
#define KEY_COPY(dst, src) ((dst) = (src))
#define KEY_GET(k, someint) ((k) = (someint))
#define KEY_ADD(dst, k1, k2) ((dst) = (k1) + (k2))
