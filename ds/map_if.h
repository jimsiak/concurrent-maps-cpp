#pragma once

/**
 * This file specifies the base class called `Map` which defines
 * the interface for all our map data structures.
**/

#include <iostream>
#include <vector>

#include "Log.h"

#define NOT_IMPLEMENTED() log_info("%s() is not yet overriden by this data structure\n", __func__)

template <typename K, typename V>
class Map {
public:
	/**
	 * The interface necessary for a Map data structure
	 **/

	Map() {};
	~Map() {};

	//> Thread initialize/finalize functions. Called by each thread that will
	//> perform operations on the map.
	virtual void initThread(const int tid) = 0;
	virtual void deinitThread(const int tid) = 0;

	//> Map operations. Thread-safe.
	virtual bool                    contains(const int tid, const K& key) = 0;
	virtual const std::pair<V,bool> find(const int tid, const K& key) = 0;
	virtual int                     rangeQuery(const int tid,
	                                           const K& lo, const K& hi,
	                                           std::vector<std::pair<K,V>> kv_pairs) = 0;

	virtual const V                 insert(const int tid, const K& key,
	                                       const V& val) = 0;
	virtual const V                 insertIfAbsent(const int tid, const K& key,
	                                               const V& val) = 0;
	virtual const std::pair<V,bool> remove(const int tid, const K& key) = 0;

	//> Functions that are called by only one thread before or after the
	//> execution of any benchmark on the map.
	virtual bool  validate() = 0;
	virtual char *name() = 0;
	
	//> Methods that are used for debugging.
	//> These are not pure virtual to allow newly implemented data structures
	//> to be used without the need to implement those methods.
	virtual void print() { NOT_IMPLEMENTED(); }
	virtual unsigned long long size() { NOT_IMPLEMENTED(); return -1; }

public:
	/**
	 * The interface necessary for RCU-HTM synchronization
	 **/

	//> FIXME maybe it is better to move all those virtual functions in
	//>       another class named e.g. rcu_htm_map

	//> In the following function definitions `void *` should be `node_t *`
	//> but we do not want this class to be aware of what `node_t` is.
	//> So we have to cast `void *` to `node_t *` inside the implementation of
	//> each function.

	virtual bool traverse_with_stack(const K& key, void **stack,
	                                 int *stack_indexes, int *stack_top) { return false; };
	virtual void install_copy(void *connpoint, void *privcopy, int *, int) {};
	virtual void validate_copy(void **node_stack, int *node_stack_indexes,
	                           int stack_top) {};
	virtual void *insert_with_copy(const K& key, const V& value, void **stack,
	                               int *stack_indexes, int stack_top, void **privcopy,
	                               int *connpoint_stack_index) { return NULL; };
	//> `*stack_top` should only be modified downwards, i.e., see bst_avl_int.h
	virtual void *delete_with_copy(const K& key, void **stack,
	                               int *stack_indexes, int *stack_top, void **privcopy,
	                               int *connpoint_stack_index) { return NULL; };

	//> These are needed only for the (a-b)-tree where we need to do rebalance
	//> asynchronously after the insertion or deletion
	virtual bool is_abtree() { return false; };
	virtual void traverse_for_rebalance(const K& key, int *should_rebalance,
	                                    void **stack, int *stack_indexes,
	                                    int *stack_top) {}; 
	virtual void *rebalance_with_copy(const K& key, void **stack,
	                                  int *stack_indexes, int stack_top,
	                                  int *should_rebalance, void **privcopy,
	                                  int *connpoint_stack_index) { return NULL; };

};
