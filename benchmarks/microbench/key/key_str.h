#ifndef _KEY_STR_H_
#define _KEY_STR_H_

#include <stdio.h>
#include <string.h>

#define STR_HELPER(x) #x
#define TO_STR(x) STR_HELPER(x)
#define SZ (STR_KEY_SZ+1)
#define FORMAT "%0"TO_STR(STR_KEY_SZ)"d"

typedef char map_key_t[SZ];
#define MAX_KEY "9999999999999999999999999999999999999999999999999"
#define MIN_KEY "0000000000000000000000000000000000000000000000000"
#define KEY_PRINT(k, PREFIX, POSTFIX) printf("%s%llu%s", (PREFIX), atoll(k), (POSTFIX))

#define KEY_CMP(k1, k2) strncmp(k1, k2, SZ-1)
#define KEY_COPY(dst, src) strncpy(dst, src, SZ)
#define KEY_GET(k, someint) snprintf(k, SZ, FORMAT, someint)
#define KEY_ADD(dst, k1, k2) strncpy(dst, k1, SZ)

#endif /* _KEY_STR_H_ */
