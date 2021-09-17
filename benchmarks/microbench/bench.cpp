#include <iostream>
#include <vector>
#include <pthread.h>

#include "Timer.h"

#define WORKLOAD_TIME

#define map_val_t void *
#define map_t Map<map_key_t, map_val_t>

#include "../../ds/map_factory.h"
#include "Keygen.h"
#include "key/key.h"

#include "clargs.h"
#include "thread_data.h"
#include "aff.h"

pthread_barrier_t start_barrier;

void *thread_fn(void *arg)
{
	thread_data_t *data = (thread_data_t *)arg;
	int ret, tid = data->tid, cpu = data->cpu;
	map_t *map = data->map;
	unsigned choice;
	map_key_t key;
#	if defined(WORKLOAD_FIXED)
	int ops_performed = 0;
#	endif
	
	//> Set affinity.
	setaffinity_oncpu(cpu);

	//> Initialize per thread Map data.
	map->initThread(tid);

	//> Initialize random number generators
	int seed = (tid + 1) * clargs.thread_seed;
	KeyGenerator *keygen = new KeyGeneratorUniform(seed, clargs.max_key);
	KeyGenerator *keygen_choice = new KeyGeneratorUniform(seed, UINT_MAX);

	//> Wait for the master to give the starting signal.
	pthread_barrier_wait(&start_barrier);

	//> Critical section.
	while (1) {
#		if defined(WORKLOAD_FIXED)
		if (ops_performed >= data->nr_operations - 1)
			break;
		ops_performed = data->operations_performed[OPS_TOTAL];
#		elif defined(WORKLOAD_TIME)
		if (*(data->time_to_leave))
			break;
#		endif

		//> Generate random number.
		choice = (unsigned)keygen_choice->next() % 100;
		KEY_GET(key, keygen->next());
		if (key == 0) key = 1;

		data->operations_performed[OPS_TOTAL]++;

		//> Perform operation on the RBT based on choice.
		if (choice < clargs.lookup_frac) {
			//> Lookup
			data->operations_performed[OPS_LOOKUP]++;
			ret = map->contains(tid, key);
			data->operations_succeeded[OPS_LOOKUP] += ret;
		} else if (choice < clargs.lookup_frac + clargs.rquery_frac) {
			//> Range-Query
			data->operations_performed[OPS_RQUERY]++;
			unsigned long long key2 = key + 10000;
			std::vector<std::pair<map_key_t, map_val_t>> kv_pairs;
			ret = map->rangeQuery(tid, key, key2, kv_pairs);
			data->operations_succeeded[OPS_RQUERY] += ret;
//		} else {
//			//> Update
//			ret = map_update(map, data->map_tdata, key, NULL);
//			if (ret == 0 || ret == 1) {
//				data->operations_performed[OPS_INSERT]++;
//				data->operations_succeeded[OPS_INSERT] += ret;
//			} else if (ret == 2 || ret == 3) {
//				ret -= 2;
//				data->operations_performed[OPS_DELETE]++;
//				data->operations_succeeded[OPS_DELETE] += ret;
//			} else {
//				log_error("Wrong return value from map_update() ret=%d\n", ret);
//				exit(1);
//			}
//		}
		} else if (choice < clargs.lookup_frac + clargs.rquery_frac
		                                       + clargs.insert_frac) {
			//> Insertion
			data->operations_performed[OPS_INSERT]++;
			map_val_t retp;
			retp = map->insertIfAbsent(tid, key, (map_val_t)key);
			ret = (retp == NULL);
			data->operations_succeeded[OPS_INSERT] += ret;
		} else {
			//> Deletion
			data->operations_performed[OPS_DELETE]++;
			std::pair<map_val_t, bool> retp;
			retp = map->remove(tid, key);
			ret = retp.second;
			data->operations_succeeded[OPS_DELETE] += ret;
		}
		data->operations_succeeded[OPS_TOTAL] += ret;
	}

	return NULL;
}

static inline int map_warmup(map_t *map, int nr_nodes, int max_key,
                             unsigned int seed)
{
    int nodes_inserted = 0;
	unsigned long long key;
	map_val_t ret;
	KeyGeneratorUniform keygen(seed, max_key);

    srand(seed);
    while (nodes_inserted < nr_nodes) {
		KEY_GET(key, keygen.next());

		ret = map->insertIfAbsent(0, key, (map_val_t)key);
        nodes_inserted += (ret == NULL);
    }

    return nodes_inserted;
}

int main(int argc, char **argv)
{
	int i, validation, nthreads;
	std::vector<pthread_t> threads;
	std::vector<thread_data_t *> threads_data;
	unsigned int ncpus, *cpus;
	int time_to_leave = 0;
	Timer warmup_timer;
	map_t *map;

	//> Read command line arguments
	clargs_init(argc, argv);
	clargs_print();
	nthreads = clargs.num_threads;


	//> Initialize the Map data structure.
	std::string map_type(clargs.ds_name);
	std::string sync_type(clargs.sync_type);
	map = createMap<map_key_t, map_val_t>(map_type, sync_type);
	log_info("Benchmark\n");
	log_info("=======================\n");
	log_info("  MAP implementation: %s\n", map->name());

	//> Initialize the warmup thread.
	int warmup_core = 0;
	setaffinity_oncpu(warmup_core);
	map->initThread(0);

	//> Map warmup.
	log_info("\n");
	log_info("Tree initialization (at core %d)...\n", warmup_core);
	warmup_timer.start();
	map_warmup(map, clargs.init_tree_size, clargs.max_key, clargs.init_seed);
	warmup_timer.stop();
	log_info("Initialization finished in %.2lf sec\n", warmup_timer.report_sec());

	//> Initialize the starting barrier.
	pthread_barrier_init(&start_barrier, NULL, nthreads+1);
	
	//> Initialize the vectors that hold the thread references and data.
	threads.reserve(nthreads);
	threads_data.reserve(nthreads);

	//> Get the mapping of threads to cpus
	log_info("\n");
	log_info("Reading MT_CONF, to get the thread->cpu mapping.\n");
	get_mtconf_options(&ncpus, &cpus);
	mt_conf_print(ncpus, cpus);

	//> Initialize per thread data and spawn threads.
	for (i=0; i < nthreads; i++) {
		int cpu = cpus[i];
		threads_data[i] = thread_data_new(i, cpu, map);
#		ifdef WORKLOAD_FIXED
		threads_data[i]->nr_operations = clargs.nr_operations / nthreads;
#		elif defined(WORKLOAD_TIME)
		threads_data[i]->time_to_leave = &time_to_leave;
#		endif
		pthread_create(&threads[i], NULL, thread_fn, threads_data[i]);
	}

	//> Wait until all threads go to the starting point.
	pthread_barrier_wait(&start_barrier);

	//> Init and start wall_timer.
	Timer wall_timer;
	wall_timer.start();

#	if defined(WORKLOAD_TIME)
	sleep(clargs.run_time_sec);
	time_to_leave = 1;
#	endif

	//> Join threads.
	for (i=0; i < nthreads; i++)
		pthread_join(threads[i], NULL);

	//> Stop wall_timer.
	wall_timer.stop();

	//> Print thread statistics.
	thread_data_t *total_data = thread_data_new(-1, -1, NULL);
	log_info("\nThread statistics\n");
	log_info("=======================\n");
	for (i=0; i < nthreads; i++) {
		thread_data_print(threads_data[i]);
		thread_data_add(threads_data[i], total_data, total_data);
	}
	log_info("-----------------------\n");
	thread_data_print(total_data);

//	//> Print additional per thread statistics.
//	total_data->map_tdata = map_tdata_new(-1);
//	log_info("\n");
//	log_info("\nAdditional per thread statistics\n");
//	log_info("=======================\n");
//	for (i=0; i < nthreads; i++) {
//		thread_data_print_map_data(threads_data[i]);
//		thread_data_add_map_data(threads_data[i], total_data, total_data);
//	}
//	log_info("-----------------------\n");
//	thread_data_print_map_data(total_data);
//	log_info("\n");

	//> Validate the final RBT.
	validation = map->validate();
//	map->print();

	//> Print elapsed time.
	double time_elapsed = wall_timer.report_sec();
	double throughput_usec = total_data->operations_performed[OPS_TOTAL] / 
	                         time_elapsed / 1000000.0;
	log_info("\n");
	log_info("Time elapsed: %6.2lf\n", time_elapsed);
	log_info("Throughput(Ops/usec): %7.3lf\n", throughput_usec);

	log_info("Expected size of MAP: %llu\n",
	        (long long unsigned)clargs.init_tree_size +
	        total_data->operations_succeeded[OPS_INSERT] - 
	        total_data->operations_succeeded[OPS_DELETE]);

	return 0;
}
