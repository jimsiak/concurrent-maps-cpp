#ifndef _KEY_BIG_INT_H_
#define _KEY_BIG_INT_H_

#include <limits.h> //> For INT_MAX

typedef struct {
	int value;
	char padding[BIG_INT_KEY_SZ - sizeof(int)];
} map_key_t;

static map_key_t max_key_var = { .value = INT_MAX };
static map_key_t min_key_var = { .value = -1};
#define MAX_KEY max_key_var
#define MIN_KEY min_key_var
#define KEY_PRINT(k, PREFIX, POSTFIX) printf("%s%d%s", (PREFIX), (k).value, (POSTFIX))

#define KEY_COPY(dst, src) ((dst).value = (src).value)
#define KEY_GET(k, someint) ((k).value = (someint))
#define KEY_ADD(dst, k1, k2) ((dst).value = (k1).value + (k2).value)

static int KEY_CMP(map_key_t k1, map_key_t k2) {
	unsigned int i;
	volatile int sum = 0;

	for (i=0; i < BIG_INT_KEY_SZ - sizeof(int); i++)
		sum += k1.padding[i] + k2.padding[i];

	return k1.value - k2.value;
}

#endif /* _KEY_BIG_INT_H_ */
