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
	   : TX_NUM_RETRIES(num_retries)
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

	int insert_helper(const K& key, const V& val)
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
			bool found = seq_ds->traverse_with_stack(key, node_stack,
			                                         node_stack_indexes, &stack_top);
			if (found) return 0;

			connection_point = seq_ds->insert_with_copy(key, val, 
			                                  node_stack, node_stack_indexes, stack_top,
			                                  &tree_cp_root,
			                                  &connection_point_stack_index);
			bool installed = validate_and_install_copy(connection_point, tree_cp_root,
			                                           node_stack, node_stack_indexes,
			                                           stack_top,
			                                           connection_point_stack_index);

			//> If its is an (a-b)-tree we should also rebalance here.
			if (seq_ds->is_abtree()) {
				if (!installed) continue;

				int should_rebalance = 1;
				while (should_rebalance) {
					ht_reset(tdata->ht);
					seq_ds->traverse_for_rebalance(key, &should_rebalance,
					                               node_stack, node_stack_indexes,
					                               &stack_top);
					if (!should_rebalance) break;
					connection_point = seq_ds->rebalance_with_copy(key,
					                           node_stack, node_stack_indexes,
					                           stack_top, &should_rebalance, &tree_cp_root,
					                           &connection_point_stack_index);
					bool installed = validate_and_install_copy(connection_point, tree_cp_root,
				                                           node_stack, node_stack_indexes,
				                                           stack_top,
				                                           connection_point_stack_index);
					if (!installed) break;
				}
			}

			if (installed) return 1;
		}

		//> ...otherwise fallback to the coarse-grained RCU
		ht_reset(tdata->ht);
		tdata->lacqs++;
		pthread_spin_lock(&updaters_lock);
		bool found = seq_ds->traverse_with_stack(key, node_stack,
		                                        node_stack_indexes, &stack_top);
		if (found) {
			pthread_spin_unlock(&updaters_lock);
			return 0;
		}
		connection_point = seq_ds->insert_with_copy(key, val, 
		                                  node_stack, node_stack_indexes, stack_top,
		                                  &tree_cp_root,
		                                  &connection_point_stack_index);
		seq_ds->install_copy(connection_point, tree_cp_root,
		                     node_stack_indexes,
		                     connection_point_stack_index);
		pthread_spin_unlock(&updaters_lock);
		return 1;
	}

	int delete_helper(const K& key)
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
			bool found = seq_ds->traverse_with_stack(key, node_stack,
			                                         node_stack_indexes, &stack_top);
			if (!found) return 0;

			connection_point = seq_ds->delete_with_copy(key,
			                                  node_stack, node_stack_indexes, &stack_top,
			                                  &tree_cp_root,
			                                  &connection_point_stack_index);
			bool installed = validate_and_install_copy(connection_point, tree_cp_root,
			                                           node_stack, node_stack_indexes,
			                                           stack_top,
			                                           connection_point_stack_index);

			//> If its is an (a-b)-tree we should also rebalance here.
			if (seq_ds->is_abtree()) {
				if (!installed) continue;

				int should_rebalance = 1;
				while (should_rebalance) {
					ht_reset(tdata->ht);
					seq_ds->traverse_for_rebalance(key, &should_rebalance,
					                               node_stack, node_stack_indexes,
					                               &stack_top);
					if (!should_rebalance) break;
					connection_point = seq_ds->rebalance_with_copy(key,
					                           node_stack, node_stack_indexes,
					                           stack_top, &should_rebalance, &tree_cp_root,
					                           &connection_point_stack_index);
					bool installed = validate_and_install_copy(connection_point, tree_cp_root,
				                                           node_stack, node_stack_indexes,
				                                           stack_top,
				                                           connection_point_stack_index);
					if (!installed) break;
				}
			}

			if (installed) return 1;
		}

		//> ...otherwise fallback to the coarse-grained RCU
		ht_reset(tdata->ht);
		tdata->lacqs++;
		pthread_spin_lock(&updaters_lock);
		bool found = seq_ds->traverse_with_stack(key, node_stack,
		                                        node_stack_indexes, &stack_top);
		if (!found) {
			pthread_spin_unlock(&updaters_lock);
			return 0;
		}
		connection_point = seq_ds->delete_with_copy(key,
		                                  node_stack, node_stack_indexes, &stack_top,
		                                  &tree_cp_root,
		                                  &connection_point_stack_index);
		seq_ds->install_copy(connection_point, tree_cp_root,
		                     node_stack_indexes,
		                     connection_point_stack_index);
		pthread_spin_unlock(&updaters_lock);
		return 1;
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
	int ret = insert_helper(key, val);
	if (ret == 1) return NULL;
	else return (void *)1;
}

RCU_HTM_TEMPL
const std::pair<V,bool> RCU_HTM_FUNCT::remove(const int tid, const K& key)
{
	int ret = delete_helper(key);
	if (ret == 0) return std::pair<V,bool>(NULL, false);
	else return std::pair<V,bool>((void*)1, true);
}

RCU_HTM_TEMPL
bool RCU_HTM_FUNCT::validate()
{
	return seq_ds->validate();
}



//int btree_update(btree_t *btree, key_t key, void *val, tdata_t *tdata)
//{
//	tm_begin_ret_t status;
//	btree_node_t *node_stack[MAX_STACK_LEN];
//	btree_node_t *connection_point, *tree_cp_root;
//	int node_stack_indexes[MAX_STACK_LEN], stack_top, index;
//	int retries = -1;
//	int connection_point_stack_index;
//	int op_is_insert = -1, ret;
//
//try_from_scratch:
//
//	ht_reset(tdata->ht);
//
//	if (++retries >= TX_NUM_RETRIES) {
//		tdata->lacqs++;
//		pthread_spin_lock(&btree->lock);
//		btree_traverse_stack(btree, key, node_stack, node_stack_indexes, &stack_top);
//		if (op_is_insert == -1) {
//			if (stack_top < 0)
//				op_is_insert = 1;
//			else if (node_stack_indexes[stack_top] < 2 * BTREE_ORDER &&
//			         node_stack[stack_top]->keys[node_stack_indexes[stack_top]] == key)
//				op_is_insert = 0;
//			else
//				op_is_insert = 1;
//		}
//		if (op_is_insert && stack_top >= 0 && node_stack_indexes[stack_top] < 2 * BTREE_ORDER &&
//		        node_stack[stack_top]->keys[node_stack_indexes[stack_top]] == key) {
//			pthread_spin_unlock(&btree->lock);
//			return 0;
//		} else if (!op_is_insert && (stack_top < 0 || 
//		            node_stack_indexes[stack_top] >= node_stack[stack_top]->no_keys ||
//		            node_stack[stack_top]->keys[node_stack_indexes[stack_top]] != key)) {
//			pthread_spin_unlock(&btree->lock);
//			return 2;
//		}
//		if (op_is_insert) {
//			connection_point = btree_insert_with_copy(key, val, 
//			                                  node_stack, node_stack_indexes, stack_top,
//			                                  &tree_cp_root,
//			                                  &connection_point_stack_index, tdata);
//			ret = 1;
//		} else {
//			connection_point = btree_delete_with_copy(key,
//			                                  node_stack, node_stack_indexes, stack_top,
//			                                  &tree_cp_root,
//			                                  &connection_point_stack_index, tdata);
//			ret = 3;
//		}
//		if (connection_point == NULL) {
//			btree->root = tree_cp_root;
//		} else {
//			index = node_stack_indexes[connection_point_stack_index];
//			connection_point->children[index] = tree_cp_root;
//		}
//		pthread_spin_unlock(&btree->lock);
//		return ret;
//	}
//
//	//> Asynchronized traversal. If key is there we can safely return.
//	btree_traverse_stack(btree, key, node_stack, node_stack_indexes, &stack_top);
//	if (op_is_insert == -1) {
//		if (stack_top < 0)
//			op_is_insert = 1;
//		else if (node_stack_indexes[stack_top] < 2 * BTREE_ORDER &&
//		         node_stack[stack_top]->keys[node_stack_indexes[stack_top]] == key)
//			op_is_insert = 0;
//		else
//			op_is_insert = 1;
//	}
//	if (op_is_insert && stack_top >= 0 && node_stack_indexes[stack_top] < 2 * BTREE_ORDER &&
//	        node_stack[stack_top]->keys[node_stack_indexes[stack_top]] == key)
//		return 0;
//	else if (!op_is_insert && (stack_top < 0 || 
//	            node_stack_indexes[stack_top] >= node_stack[stack_top]->no_keys ||
//	            node_stack[stack_top]->keys[node_stack_indexes[stack_top]] != key))
//		return 2;
//
//	if (op_is_insert) {
//		connection_point = btree_insert_with_copy(key, val, 
//		                                  node_stack, node_stack_indexes, stack_top,
//		                                  &tree_cp_root,
//		                                  &connection_point_stack_index, tdata);
//		ret = 1;
//	} else {
//		connection_point = btree_delete_with_copy(key,
//		                                  node_stack, node_stack_indexes, stack_top,
//		                                  &tree_cp_root,
//		                                  &connection_point_stack_index, tdata);
//		ret = 3;
//	}
//
//	int validation_retries = -1;
//validate_and_connect_copy:
//
//	if (++validation_retries >= TX_NUM_RETRIES) goto try_from_scratch;
//	while (btree->lock != LOCK_FREE) ;
//
//	tdata->tx_starts++;
//	status = TX_BEGIN(0);
//	if (status == TM_BEGIN_SUCCESS) {
//		if (btree->lock != LOCK_FREE)
//			TX_ABORT(ABORT_GL_TAKEN);
//
//		//> Validate copy
//		if (stack_top < 0 && btree->root != NULL)
//			TX_ABORT(ABORT_VALIDATION_FAILURE);
//		if (stack_top >= 0 && btree->root != node_stack[0])
//			TX_ABORT(ABORT_VALIDATION_FAILURE);
//		int i;
//		btree_node_t *n1, *n2;
//		for (i=0; i < stack_top; i++) {
//			n1 = node_stack[i];
//			index = node_stack_indexes[i];
//			n2 = n1->children[index];
//			if (n2 != node_stack[i+1])
//				TX_ABORT(ABORT_VALIDATION_FAILURE);
//		}
//		int j;
//		for (i=0; i < HT_LEN; i++) {
//			for (j=0; j < tdata->ht->bucket_next_index[i]; j+=2) {
//				btree_node_t **np = tdata->ht->entries[i][j];
//				btree_node_t  *n  = tdata->ht->entries[i][j+1];
//				if (*np != n) TX_ABORT(ABORT_VALIDATION_FAILURE);
//			}
//		}
//
//		// Now let's 'commit' the tree copy onto the original tree.
//		if (connection_point == NULL) {
//			btree->root = tree_cp_root;
//		} else {
//			index = node_stack_indexes[connection_point_stack_index];
//			connection_point->children[index] = tree_cp_root;
//		}
//		TX_END(0);
//	} else {
//		tdata->tx_aborts++;
//		if (ABORT_IS_EXPLICIT(status) && 
//		    ABORT_CODE(status) == ABORT_VALIDATION_FAILURE) {
//			tdata->tx_aborts_explicit_validation++;
//			goto try_from_scratch;
//		} else {
//			goto validate_and_connect_copy;
//		}
//	}
//
//	return ret;
//}
