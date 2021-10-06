/**
 * A sequential data structure wrapped in RCU-HTM synchronization.
 **/

#pragma once

#include <pthread.h>
#include "../map_if.h"
#include "Log.h"

#include "ht.h"

template <typename K, typename V>
class rcu_htm : public Map<K,V> {
public:
	rcu_htm(const K _NO_KEY, const V _NO_VALUE, const int numProcesses, Map<K,V> *seq_ds,
	        const int num_retries = 10)
	   : Map<K,V>(_NO_KEY, _NO_VALUE), TX_NUM_RETRIES(num_retries)
	{
		this->seq_ds = seq_ds;
		pthread_spin_init(&updaters_lock, PTHREAD_PROCESS_SHARED);
	}

	void initThread(const int tid) { tdata = tdata_new(tid); tdata->ht = ht_new(); };
	void deinitThread(const int tid) {};

	bool                    contains(const int tid, const K& key);
	const std::pair<V,bool> find(const int tid, const K& key);
	int                     rangeQuery(const int tid,
	                                   const K& lo, const K& hi,
	                                   std::vector<std::pair<K,V>> kv_pairs);

	const V                 insert(const int tid, const K& key, const V& val);
	const V                 insertIfAbsent(const int tid, const K& key,
	                                       const V& val);
	const std::pair<V,bool> remove(const int tid, const K& key);

	bool  validate();
	char *name()
	{
		char *seqds= seq_ds->name();
		char *name = new char[60];
		const char *sync = (TX_NUM_RETRIES > 0) ? "RCU-HTM" : "RCU-SGL";
		sprintf(name, "%s (%s) [%d retries]", seqds, sync, TX_NUM_RETRIES);
		return name;
	}

	void print() { seq_ds->print(); }
	unsigned long long size() { return seq_ds->size(); }

private:
	const int MAX_STACK_LEN = 64;
	const int TX_NUM_RETRIES; //> FIXME
	Map<K,V> *seq_ds;
	char padding[64];

	pthread_spinlock_t updaters_lock;

	bool validate_and_install_copy(void *connpoint, void *tree_cp_root,
	                               void **node_stack, int *node_stack_indexes,
	                               int stack_top, int connection_point_stack_index)
	{
		tm_begin_ret_t status;
		int validation_retries = -1;

		while (++validation_retries < TX_NUM_RETRIES) {
			while (updaters_lock != LOCK_FREE) ;

			tdata->tx_starts++;
			status = TX_BEGIN(0);
			if (status == TM_BEGIN_SUCCESS) {
				if (updaters_lock != LOCK_FREE)
					TX_ABORT(ABORT_GL_TAKEN);
		
				seq_ds->validate_copy(node_stack, node_stack_indexes, stack_top);
				seq_ds->install_copy(connpoint, tree_cp_root,
				                     node_stack_indexes,
				                     connection_point_stack_index);
				TX_END(0);
				return true;
			} else {
				tdata->tx_aborts++;
				if (ABORT_IS_EXPLICIT(status) && 
				    ABORT_CODE(status) == ABORT_VALIDATION_FAILURE) {
					tdata->tx_aborts_explicit_validation++;
					return false;
				} else {
					continue;
				}
			}
		}

		return false;
	}

	const V insert_helper(const K& key, const V& val)
	{
		void *node_stack[MAX_STACK_LEN];
		void *connection_point, *tree_cp_root;
		int node_stack_indexes[MAX_STACK_LEN], stack_top, index;
		int retries = -1;
		int connection_point_stack_index;
	
		//> First try with the RCU-HTM way ...
		while (++retries < TX_NUM_RETRIES) {
			ht_reset(tdata->ht);
	
			//> Asynchronized traversal. If key is there we can safely return.
			const V ret = seq_ds->traverse_with_stack(key, node_stack,
			                                          node_stack_indexes, &stack_top);
			assert(stack_top < MAX_STACK_LEN);
			if (ret != this->NO_VALUE) return ret;

			connection_point = seq_ds->insert_with_copy(key, val, 
			                                  node_stack, node_stack_indexes, stack_top,
			                                  &tree_cp_root,
			                                  &connection_point_stack_index);
			bool installed = validate_and_install_copy(connection_point, tree_cp_root,
			                                           node_stack, node_stack_indexes,
			                                           stack_top,
			                                           connection_point_stack_index);
			if (installed) return this->NO_VALUE;
		}

		//> ...otherwise fallback to the coarse-grained RCU
		ht_reset(tdata->ht);
		tdata->lacqs++;
		pthread_spin_lock(&updaters_lock);
		const V ret = seq_ds->traverse_with_stack(key, node_stack,
		                                          node_stack_indexes, &stack_top);
		assert(stack_top < MAX_STACK_LEN);
		if (ret != this->NO_VALUE) {
			pthread_spin_unlock(&updaters_lock);
			return ret;
		}
		connection_point = seq_ds->insert_with_copy(key, val, 
		                                  node_stack, node_stack_indexes, stack_top,
		                                  &tree_cp_root,
		                                  &connection_point_stack_index);
		seq_ds->install_copy(connection_point, tree_cp_root,
		                     node_stack_indexes,
		                     connection_point_stack_index);
		pthread_spin_unlock(&updaters_lock);
		return this->NO_VALUE;
	}

	const V delete_helper(const K& key)
	{
		void *node_stack[MAX_STACK_LEN];
		void *connection_point, *tree_cp_root;
		int node_stack_indexes[MAX_STACK_LEN], stack_top, index;
		int retries = -1;
		int connection_point_stack_index;
	
		//> First try with the RCU-HTM way ...
		while (++retries < TX_NUM_RETRIES) {
			ht_reset(tdata->ht);
	
			//> Asynchronized traversal. If key is there we can safely return.
			const V ret = seq_ds->traverse_with_stack(key, node_stack,
			                                          node_stack_indexes, &stack_top);
			assert(stack_top < MAX_STACK_LEN);
			if (ret == this->NO_VALUE) return this->NO_VALUE;

			connection_point = seq_ds->delete_with_copy(key,
			                                  node_stack, node_stack_indexes, &stack_top,
			                                  &tree_cp_root,
			                                  &connection_point_stack_index);
			bool installed = validate_and_install_copy(connection_point, tree_cp_root,
			                                           node_stack, node_stack_indexes,
			                                           stack_top,
			                                           connection_point_stack_index);
			if (installed) return ret;
		}

		//> ...otherwise fallback to the coarse-grained RCU
		ht_reset(tdata->ht);
		tdata->lacqs++;
		pthread_spin_lock(&updaters_lock);
		const V ret = seq_ds->traverse_with_stack(key, node_stack,
		                                          node_stack_indexes, &stack_top);
		assert(stack_top < MAX_STACK_LEN);
		if (ret == this->NO_VALUE) {
			pthread_spin_unlock(&updaters_lock);
			return this->NO_VALUE;
		}
		connection_point = seq_ds->delete_with_copy(key,
		                                  node_stack, node_stack_indexes, &stack_top,
		                                  &tree_cp_root,
		                                  &connection_point_stack_index);
		seq_ds->install_copy(connection_point, tree_cp_root,
		                     node_stack_indexes,
		                     connection_point_stack_index);
		pthread_spin_unlock(&updaters_lock);
		return ret;
	}

	void rebalance_helper(const K& key)
	{
		void *node_stack[MAX_STACK_LEN];
		void *connection_point, *tree_cp_root;
		int node_stack_indexes[MAX_STACK_LEN], stack_top, index;
		int retries = -1;
		int connection_point_stack_index;
		int should_rebalance = 1;

		AGAIN:
		while (should_rebalance) {
			//> First try with the RCU-HTM way ...
			while (++retries < TX_NUM_RETRIES) {
				ht_reset(tdata->ht);
				seq_ds->traverse_for_rebalance(key, &should_rebalance,
				                               node_stack, node_stack_indexes,
				                               &stack_top);
				assert(stack_top < MAX_STACK_LEN);
				if (!should_rebalance) goto AGAIN;
				connection_point = seq_ds->rebalance_with_copy(key,
				                           node_stack, node_stack_indexes,
				                           stack_top, &should_rebalance, &tree_cp_root,
				                           &connection_point_stack_index);
				bool installed = validate_and_install_copy(connection_point, tree_cp_root,
			                                           node_stack, node_stack_indexes,
			                                           stack_top,
			                                           connection_point_stack_index);
				if (installed) goto AGAIN;
			}

			//> ...otherwise fallback to the coarse-grained RCU
			ht_reset(tdata->ht);
			tdata->lacqs++;
			pthread_spin_lock(&updaters_lock);
			seq_ds->traverse_for_rebalance(key, &should_rebalance,
			                               node_stack, node_stack_indexes,
			                               &stack_top);
			assert(stack_top < MAX_STACK_LEN);
			if (!should_rebalance) {
				pthread_spin_unlock(&updaters_lock);
				break;
			}
			connection_point = seq_ds->rebalance_with_copy(key,
			                                  node_stack, node_stack_indexes, stack_top,
			                                  &should_rebalance, &tree_cp_root,
			                                  &connection_point_stack_index);
			seq_ds->install_copy(connection_point, tree_cp_root,
			                     node_stack_indexes,
			                     connection_point_stack_index);
			pthread_spin_unlock(&updaters_lock);
		}
	}
};

#define RCU_HTM_TEMPL template<typename K, typename V>
#define RCU_HTM_FUNCT rcu_htm<K,V>

RCU_HTM_TEMPL
bool RCU_HTM_FUNCT::contains(const int tid, const K& key)
{
	return seq_ds->contains(tid, key);
}

RCU_HTM_TEMPL
const std::pair<V,bool> RCU_HTM_FUNCT::find(const int tid, const K& key)
{
	return seq_ds->find(tid, key);
}

RCU_HTM_TEMPL
int RCU_HTM_FUNCT::rangeQuery(const int tid, const K& lo, const K& hi,
                      std::vector<std::pair<K,V>> kv_pairs)
{
	return seq_ds->rangeQuery(tid, lo, hi, kv_pairs);
}

RCU_HTM_TEMPL
const V RCU_HTM_FUNCT::insert(const int tid, const K& key, const V& val)
{
	return insertIfAbsent(tid, key, val);
}

RCU_HTM_TEMPL
const V RCU_HTM_FUNCT::insertIfAbsent(const int tid, const K& key, const V& val)
{
	const V ret = insert_helper(key, val);
	if (seq_ds->is_abtree() && ret == this->NO_VALUE) rebalance_helper(key);
	return ret;
}

RCU_HTM_TEMPL
const std::pair<V,bool> RCU_HTM_FUNCT::remove(const int tid, const K& key)
{
	const V ret = delete_helper(key);
	if (seq_ds->is_abtree() && ret != this->NO_VALUE) rebalance_helper(key);
	return std::pair<V,bool>(ret, ret != this->NO_VALUE);
}

RCU_HTM_TEMPL
bool RCU_HTM_FUNCT::validate()
{
	return seq_ds->validate();
}
