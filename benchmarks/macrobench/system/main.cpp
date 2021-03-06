#include "global.h"
#include "ycsb.h"
#include "tpcc.h"
#include "test.h"
#include "thread.h"
#include "manager.h"
#include "mem_alloc.h"
#include "query.h"
#include "plock.h"
#include "occ.h"
#include "vll.h"

using namespace std;

void parser(int argc, char * argv[]);

void *f_warmup(void *);
void *f_real  (void *);

void print_ccalg_and_isolation_level();
void do_cleanup(workload *);

thread_t **m_thds;

int main(int argc, char *argv[])
{
	parser(argc, argv);

	thread_pinning::configurePolicy(g_thread_cnt, g_thr_pinning_policy);
	
	papi_init_program(g_thread_cnt);
	mem_allocator.init(g_part_cnt, MEM_SIZE / g_part_cnt);
	stats.init();
	glob_manager = (Manager *) _mm_malloc(sizeof(Manager), ALIGNMENT);
	glob_manager->init();
	if (g_cc_alg == DL_DETECT) dl_detector.init();
	printf("mem_allocator initialized!\n");

	workload *m_wl;
	switch (WORKLOAD) {
		case YCSB :
			m_wl = new ycsb_wl;
			printf("Running YCSB workload\n");
			break;
		case TPCC :
			m_wl = new tpcc_wl;
			#ifdef READ_ONLY
			printf("Running READ ONLY TPCC workload\n");
			#else
			printf("Running TPCC workload\n");
			#endif
			break;
		case TEST :
			m_wl = new TestWorkload;
			((TestWorkload *)m_wl)->tick();
			printf("Running TEST workload\n");
			break;
		default:
			assert(false);
	}
	m_wl->init();
	printf("Workload initialized!\n");

	print_ccalg_and_isolation_level();

	uint64_t thd_cnt = g_thread_cnt;
	printf("running %d threads\n", g_thread_cnt);

	pthread_t p_thds[thd_cnt];
	m_thds = new thread_t *[thd_cnt];
	for (uint32_t i = 0; i < thd_cnt; i++) {
		m_thds[i] = (thread_t *) _mm_malloc(sizeof(thread_t), ALIGNMENT);
		stats.init(i);
	}

	// query_queue should be the last one to be initialized!!!
	// because it collects txn latency
	query_queue = (Query_queue *) _mm_malloc(sizeof(Query_queue), ALIGNMENT);
	if (WORKLOAD != TEST) query_queue->init(m_wl);
	pthread_barrier_init(&warmup_bar, NULL, g_thread_cnt);
	printf("query_queue initialized!\n");

	#if CC_ALG == HSTORE
	part_lock_man.init();
	#elif CC_ALG == OCC
	occ_man.init();
	#elif CC_ALG == VLL
	vll_man.init();
	#endif

	for (uint32_t i = 0; i < thd_cnt; i++)
		m_thds[i]->init(i, m_wl);

	if (WARMUP > 0) {
		printf("WARMUP start!\n");
		for (uint32_t i = 0; i < thd_cnt; i++)
			pthread_create(&p_thds[i], NULL, f_warmup, (void *)((uint64_t)i));
		for (uint32_t i = 0; i < thd_cnt; i++)
			pthread_join(p_thds[i], NULL);
		printf("WARMUP finished!\n");
	}
	warmup_finish = true;
	pthread_barrier_init(&warmup_bar, NULL, g_thread_cnt);
	pthread_barrier_init(&warmup_bar, NULL, g_thread_cnt);

	// spawn and run txns again.
	for (uint32_t i = 0; i < thd_cnt; i++)
		pthread_create(&p_thds[i], NULL, f_real, (void *)((uint64_t)i));
	for (uint32_t i = 0; i < thd_cnt; i++)
		pthread_join(p_thds[i], NULL);
	
	#ifdef VERBOSE_1
	for (map<string,Index*>::iterator it = m_wl->indexes.begin(); it!=m_wl->indexes.end(); it++) {
		printf("Index: %s\n", it->first.c_str());
		it->second->printLockCounts();
	}
	#endif
	for (map<string,Index*>::iterator it = m_wl->indexes.begin(); it!=m_wl->indexes.end(); it++) {
		printf("Index: %s\n", it->first.c_str());
		it->second->print_stats();
	}

	if (WORKLOAD != TEST) if (STATS_ENABLE) stats.print(m_wl);
	else ((TestWorkload *)m_wl)->summarize();

	do_cleanup(m_wl);

	return 0;
}

void *f_warmup(void *id)
{
	uint64_t __tid = (uint64_t)id;
	tid = __tid;
	thread_pinning::bindThread(__tid);
	#ifdef VERBOSE_1
	cout<<"WARMUP: Assigned thread ID="<<tid<<std::endl;
	#endif
	m_thds[__tid]->_wl->initThread(tid);
	m_thds[__tid]->run();
	m_thds[__tid]->_wl->deinitThread(tid);
	return NULL;
}

void *f_real(void *id)
{
	uint64_t __tid = (uint64_t)id;
	tid = __tid;
	thread_pinning::bindThread(__tid);
	papi_create_eventset(__tid);
	#ifdef VERBOSE_1
	cout<<"REAL: Assigned thread ID="<<tid<<std::endl;
	#endif
	m_thds[__tid]->_wl->initThread(tid);
	m_thds[__tid]->run();
	m_thds[__tid]->_wl->deinitThread(tid);
	return NULL;
}

void print_ccalg_and_isolation_level()
{
	switch (CC_ALG) {
		case NO_WAIT:
			printf("using NO_WAIT concurrency control\n");
			break;
		case WAIT_DIE:
			printf("using WAIT_DIE concurrency control\n");
			break;
		case DL_DETECT:
			printf("using DL_DETECT concurrency control\n");
			break;
		case TIMESTAMP:
			printf("using TIMESTAMP concurrency control\n");
			break;
		case MVCC:
			printf("using MVCC concurrency control\n");
			break;
		case HSTORE:
			printf("using HSTORE concurrency control\n");
			break;
		case OCC:
			printf("using OCC concurrency control\n");
			break;
		case TICTOC:
			printf("using TICTOC concurrency control\n");
			break;
		case SILO:
			printf("using SILO concurrency control\n");
			break;
		case VLL:
			printf("using VLL concurrency control\n");
			break;
		case HEKATON:
			printf("using HEKATON concurrency control\n");
			break;
	}

	switch (ISOLATION_LEVEL) {
		case SERIALIZABLE:
			printf("using SERIALIZABLE isolation level\n");
			break;
		case SNAPSHOT:
			printf("using SNAPSHOT isolation level\n");
			break;
		case REPEATABLE_READ:
			printf("using REPEATABLE_READ isolation level\n");
			break;
	}
}

void do_cleanup(workload *m_wl)
{
	/*********************************************************************
	* CLEANUP DATA TO ENSURE WE HAVEN'T MISSED ANY LEAKS
	* This was notably missing in DBx1000...
	********************************************************************/
//	#if !defined NO_CLEANUP_AFTER_WORKLOAD
	#if 0

	// free indexes
	for (map<string,Index*>::iterator it = m_wl->indexes.begin(); it!=m_wl->indexes.end(); it++) {
		printf("\n\ndeleting index: %s\n", it->first.c_str());
		it->second->~Index();
		free(it->second);
	}

	if (glob_manager) {
		free(glob_manager);
		glob_manager = NULL;

	}
	if (m_thds) {
		for (uint32_t i = 0; i < thd_cnt; i++) free(m_thds[i]);
		delete[] m_thds;
	}

	if (WORKLOAD != TEST) {
		if (query_queue) {
			free(query_queue);
			query_queue = NULL;
		}
	}

	if (m_wl) {
		delete m_wl;
		m_wl = NULL;
	}

	#endif
}
