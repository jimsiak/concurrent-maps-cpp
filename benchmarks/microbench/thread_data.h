#pragma once

#include <cstring>

#define CACHE_LINE_SIZE 64

//> Enumeration for indexing the operations_{performed,succeeded} arrays.
enum {
	OPS_TOTAL = 0,
	OPS_LOOKUP,
	OPS_RQUERY,
	OPS_INSERT,
	OPS_DELETE,
	OPS_END
};

typedef struct {
	int tid;
	int cpu;
	
	map_t *map;

#	if defined(WORKLOAD_FIXED)
	int nr_operations;
#	elif defined(WORKLOAD_TIME)
	int *time_to_leave;
#	endif

	unsigned long long operations_performed[OPS_END],
	                   operations_succeeded[OPS_END];

	void *map_tdata;

	char padding[2*CACHE_LINE_SIZE - 3*sizeof(int) - 2*sizeof(void *) -
	                               2*OPS_END*sizeof(unsigned long long)];
} __attribute__((aligned(CACHE_LINE_SIZE))) thread_data_t;

static inline thread_data_t *thread_data_new(int tid, int cpu, map_t *map)
{
	thread_data_t *ret = new thread_data_t();

	memset(ret, 0, sizeof(*ret));
	ret->tid = tid;
	ret->cpu = cpu;
	ret->map = map;

	return ret;
}

static inline void thread_data_print(thread_data_t *data)
{
	int i;
	printf("%3d %3d", data->tid, data->cpu);
	for (i=0; i < OPS_END; i++)
		printf(" %14llu %14llu", data->operations_performed[i], 
		                         data->operations_succeeded[i]);
	printf("\n");
}

static inline void thread_data_print_map_data(thread_data_t *data)
{
//	map_tdata_print(data->map_tdata);
}

static inline void thread_data_add_map_data(thread_data_t *d1, thread_data_t *d2,
                              thread_data_t *dst)
{
//	map_tdata_add(d1->map_tdata, d2->map_tdata, dst->map_tdata);
}

static inline void thread_data_add(thread_data_t *d1, thread_data_t *d2, 
                                   thread_data_t *dest)
{
	int i = 0;

#	if defined(WORKLOAD_FIXED)
	dest->nr_operations = d1->nr_operations + d2->nr_operations;
#	endif

	for (i=0; i < OPS_END; i++) {
		dest->operations_performed[i] = d1->operations_performed[i] + 
		                                d2->operations_performed[i];
		dest->operations_succeeded[i] = d1->operations_succeeded[i] + 
		                                d2->operations_succeeded[i];
	}
}
