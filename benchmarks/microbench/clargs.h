#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include <getopt.h>

#include "Log.h"

/**
 * The interface provided
 **/
static void clargs_init(int argc, char **argv);
static void clargs_print();

typedef struct {
	unsigned num_threads,
	         init_tree_size,
	         max_key,
	         lookup_frac,
	         rquery_frac,
	         insert_frac,
	         init_seed,
	         thread_seed;

	char *ds_name;
	char *sync_type;

#	ifdef WORKLOAD_TIME
	int run_time_sec;
#	elif defined(WORKLOAD_FIXED)
	int nr_operations;
#	endif
} clargs_t;

//>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
//> Default command line arguments
//>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
#define ARGUMENT_DEFAULT_NUM_THREADS 1
#define ARGUMENT_DEFAULT_INIT_TREE_SIZE 100000
#define ARGUMENT_DEFAULT_MAX_KEY (2 * ARGUMENT_DEFAULT_INIT_TREE_SIZE) 
#define ARGUMENT_DEFAULT_LOOKUP_FRAC 0
#define ARGUMENT_DEFAULT_RQUERY_FRAC 0
#define ARGUMENT_DEFAULT_INSERT_FRAC 50
#define ARGUMENT_DEFAULT_INIT_SEED 1024
#define ARGUMENT_DEFAULT_THREAD_SEED 128
#define ARGUMENT_DEFAULT_DS_NAME "bst-unb-ext"
#define ARGUMENT_DEFAULT_SYNC_TYPE "Sequential"
#ifdef WORKLOAD_TIME
#define ARGUMENT_DEFAULT_RUN_TIME_SEC 5
#elif defined WORKLOAD_FIXED
#define ARGUMENT_DEFAULT_NR_OPERATIONS 1000000
#endif

static char *opt_string = "ht:s:m:i:l:q:r:e:j:o:d:f:";
static struct option long_options[] = {
	{ "help",            no_argument,       NULL, 'h' },
	{ "num-threads",     required_argument, NULL, 't' },
	{ "init-tree",       required_argument, NULL, 's' },
	{ "max-key",         required_argument, NULL, 'm' },
	{ "lookup-frac",     required_argument, NULL, 'l' },
	{ "rquery-frac",     required_argument, NULL, 'q' },
	{ "insert-frac",     required_argument, NULL, 'i' },
	{ "init-seed",       required_argument, NULL, 'e' },
	{ "thread-seed",     required_argument, NULL, 'j' },
	{ "ds-name",         required_argument, NULL, 'd' },
	{ "sync-type",       required_argument, NULL, 'f' },

#	if defined(WORKLOAD_FIXED)
	{ "nr-operations",   required_argument, NULL, 'o' },
#	elif defined(WORKLOAD_TIME)
	{ "run-time-sec",    required_argument, NULL, 'r' },
#	endif

	{ NULL, 0, NULL, 0 }
};

static clargs_t clargs = {
	ARGUMENT_DEFAULT_NUM_THREADS,
	ARGUMENT_DEFAULT_INIT_TREE_SIZE,
	ARGUMENT_DEFAULT_MAX_KEY,
	ARGUMENT_DEFAULT_LOOKUP_FRAC,
	ARGUMENT_DEFAULT_RQUERY_FRAC,
	ARGUMENT_DEFAULT_INSERT_FRAC,
	ARGUMENT_DEFAULT_INIT_SEED,
	ARGUMENT_DEFAULT_THREAD_SEED,
	ARGUMENT_DEFAULT_DS_NAME,
	ARGUMENT_DEFAULT_SYNC_TYPE,
#	ifdef WORKLOAD_TIME
	ARGUMENT_DEFAULT_RUN_TIME_SEC
#	elif defined(WORKLOAD_FIXED)
	ARGUMENT_DEFAULT_NR_OPERATIONS
#	endif
};

static void clargs_print_usage(char *progname)
{
	log_info("usage: %s [options]\n", progname);
	log_info("  possible options:\n");
	log_info("    -h,--help  print this help message\n");
	log_info("    -t,--num-threads  number of threads [%d]\n",
	         ARGUMENT_DEFAULT_NUM_THREADS);
	log_info("    -s,--init-tree  number of elements the initial tree contains [%d]\n",
	         ARGUMENT_DEFAULT_INIT_TREE_SIZE);
	log_info("    -m,--max-key  max key to lookup,insert,delete [%d]\n",
	         ARGUMENT_DEFAULT_MAX_KEY);
	log_info("    -l,--lookup-frac  lookup fraction of operations [%d%%]\n",
	         ARGUMENT_DEFAULT_LOOKUP_FRAC);
	log_info("    -q,--rquery-frac  rquery fraction of operations [%d%%]\n",
	         ARGUMENT_DEFAULT_RQUERY_FRAC);
	log_info("    -i,--insert-frac  insert fraction of operations [%d%%]\n",
	         ARGUMENT_DEFAULT_INSERT_FRAC);
	log_info("    -e,--init-seed    the seed that is used for the tree initializion [%d]\n",
	         ARGUMENT_DEFAULT_INIT_SEED);
	log_info("    -j,--thread-seed  the seed that is used for the thread operations [%d]\n",
	         ARGUMENT_DEFAULT_THREAD_SEED);
	log_info("    -d,--ds-name  the name of the data structure to be used [%s]\n",
	         ARGUMENT_DEFAULT_DS_NAME);
	log_info("    -f,--sync-type  the synchronization mechanism to be used [%s]\n",
	         ARGUMENT_DEFAULT_SYNC_TYPE);

#	ifdef WORKLOAD_TIME
	log_info("    -r,--run-time-sec execution time [%d sec]\n",
	        ARGUMENT_DEFAULT_RUN_TIME_SEC);
#	elif defined(WORKLOAD_FIXED)
	log_info("    -o,--nr-operations number of operations to execute [%d]\n",
	        ARGUMENT_DEFAULT_NR_OPERATIONS);
#	endif
}

static void clargs_init(int argc, char **argv)
{
	char c;
	int i;

	while (1) {
		i = 0;
		c = getopt_long(argc, argv, opt_string, long_options, &i);
		if (c == -1)
			break;

		switch(c) {
		case 'h':
			clargs_print_usage(argv[0]);
			exit(1);
		case 't':
			clargs.num_threads = atoi(optarg);
			break;
		case 's':
			clargs.init_tree_size = atoi(optarg);
			break;
		case 'm':
			clargs.max_key = atoi(optarg);
			break;
		case 'l':
			clargs.lookup_frac = atoi(optarg);
			break;
		case 'q':
			clargs.rquery_frac = atoi(optarg);
			break;
		case 'i':
			clargs.insert_frac = atoi(optarg);
			break;
		case 'e':
			clargs.init_seed = atoi(optarg);
			break;
		case 'j':
			clargs.thread_seed = atoi(optarg);
			break;
		case 'd':
			clargs.ds_name = optarg;
			break;
		case 'f':
			clargs.sync_type = optarg;
			break;
#		ifdef WORKLOAD_TIME
		case 'r':
			clargs.run_time_sec = atoi(optarg);
			break;
#		elif defined(WORKLOAD_FIXED)
		case 'o':
			clargs.nr_operations = atoi(optarg);
			break;
#		endif
		default:
			clargs_print_usage(argv[0]);
			exit(1);
		}
	}

	/* Sanity checks. */
	assert(clargs.lookup_frac + clargs.rquery_frac + clargs.insert_frac <= 100);
}

static void clargs_print()
{
	log_info("Inputs:\n");
	log_info("====================\n");
	log_info("  num_threads: %d\n", clargs.num_threads);
	log_info("  init_tree_size: %d\n", clargs.init_tree_size);
	log_info("  max_key: %d\n", clargs.max_key);
	log_info("  lookup_frac: %d\n", clargs.lookup_frac);
	log_info("  rqery_frac: %d\n", clargs.rquery_frac);
	log_info("  insert_frac: %d\n", clargs.insert_frac);
	log_info("  init_seed: %d\n", clargs.init_seed);
	log_info("  thread_seed: %d\n", clargs.thread_seed);
	log_info("  ds_name: %s\n", clargs.ds_name);
	log_info("  sync_type: %s\n", clargs.sync_type);

#	ifdef WORKLOAD_TIME
	log_info("  run_time_sec: %d\n", clargs.run_time_sec);
#	elif defined(WORKLOAD_FIXED)
	log_info("  nr_operations: %d\n", clargs.nr_operations);
#	endif

	log_info("\n");
}
