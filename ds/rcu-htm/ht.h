#pragma once

#include <cstdio>
#include <cstring>

//> FIXME
#include <immintrin.h>
typedef unsigned tm_begin_ret_t;
#define LOCK_FREE 1
#define TM_BEGIN_SUCCESS _XBEGIN_STARTED
#define ABORT_VALIDATION_FAILURE 0xee
#define ABORT_GL_TAKEN           0xff
#define ABORT_IS_CONFLICT(status) ((status) & _XABORT_CONFLICT)
#define ABORT_IS_EXPLICIT(status) ((status) & _XABORT_EXPLICIT)
#define ABORT_CODE(status) _XABORT_CODE(status)
#define TX_ABORT(code) _xabort(code)
#define TX_BEGIN(code) _xbegin()
#define TX_END(code)   _xend()


/******************************************************************************/
/* A simple hash table implementation.                                        */
/******************************************************************************/
#define HT_LEN 16
#define HT_MAX_BUCKET_LEN 64
#define HT_GET_BUCKET(key) ((((long long)(key)) >> 4) % HT_LEN)
typedef struct {
	unsigned short bucket_next_index[HT_LEN];
	// Even numbers (0,2,4) are keys, odd numbers are values.
	void *entries[HT_LEN][HT_MAX_BUCKET_LEN * 2];
} ht_t;

static ht_t *ht_new()
{
	int i;
	ht_t *ret;

	ret = new ht_t();
	memset(&ret->bucket_next_index[0], 0, sizeof(ret->bucket_next_index));
	memset(&ret->entries[0][0], 0, sizeof(ret->entries));
	return ret;
}

static void ht_reset(ht_t *ht)
{
	memset(&ht->bucket_next_index[0], 0, sizeof(ht->bucket_next_index));
}

static void ht_insert(ht_t *ht, void *key, void *value)
{
	int bucket = HT_GET_BUCKET(key);
	unsigned short bucket_index = ht->bucket_next_index[bucket];

	ht->bucket_next_index[bucket] += 2;

	assert(bucket_index < HT_MAX_BUCKET_LEN * 2);

	ht->entries[bucket][bucket_index] = key;
	ht->entries[bucket][bucket_index+1] = value;
}

static void *ht_get(ht_t *ht, void *key)
{
	int bucket = HT_GET_BUCKET(key);
	int i;

	for (i=0; i < ht->bucket_next_index[bucket]; i+=2)
		if (key == ht->entries[bucket][i])
			return ht->entries[bucket][i+1];

	return NULL;
}

static void ht_print(ht_t *ht)
{
	int i, j;

	for (i=0; i < HT_LEN; i++) {
		printf("BUCKET[%3d]:", i);
		for (j=0; j < ht->bucket_next_index[i]; j+=2)
			printf(" (%p, %p)", ht->entries[i][j], ht->entries[i][j+1]);
		printf("\n");
	}
}
/******************************************************************************/



/******************************************************************************/
//> Transaction begin/end and data.
/******************************************************************************/
typedef struct {
	int tid;
	long long unsigned tx_starts, tx_aborts, 
	                   tx_aborts_explicit_validation, lacqs;
	ht_t *ht;
} tdata_t;

static inline tdata_t *tdata_new(int tid)
{
	tdata_t *ret = new tdata_t();
	ret->tid = tid;
	ret->tx_starts = 0;
	ret->tx_aborts = 0;
	ret->tx_aborts_explicit_validation = 0;
	ret->lacqs = 0;
	ret->ht = ht_new();
	return ret;
}

static inline void tdata_print(tdata_t *tdata)
{
	printf("TID %3d: %llu %llu %llu ( %llu )\n", tdata->tid, tdata->tx_starts,
	      tdata->tx_aborts, tdata->tx_aborts_explicit_validation, tdata->lacqs);
}

static inline void tdata_add(tdata_t *d1, tdata_t *d2, tdata_t *dst)
{
	dst->tx_starts = d1->tx_starts + d2->tx_starts;
	dst->tx_aborts = d1->tx_aborts + d2->tx_aborts;
	dst->tx_aborts_explicit_validation = d1->tx_aborts_explicit_validation +
	                                     d2->tx_aborts_explicit_validation;
	dst->lacqs = d1->lacqs + d2->lacqs;
}
/******************************************************************************/

static __thread tdata_t *tdata;
