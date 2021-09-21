#pragma once

#include <string>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdio.h>
#include <assert.h>
#include <pthread.h>
#include <vector>

#include "../map_if.h"
#include "Stack.h"

#define STAT_LOCK_HIGH_CONTENTION_LIMIT 1000
#define STAT_LOCK_LOW_CONTENTION_LIMIT -1000
#define STAT_LOCK_FAIL_CONTRIB 250
#define STAT_LOCK_SUCC_CONTRIB 1

template<typename K, typename V>
class ca_locks : public Map<K,V> {
private:
	//> Kept here for use in name()
	char *seq_ds_name;

	//> This is the base class for a CA node.
	class caNode {
	public:
		bool is_valid_;
		bool is_route_;
	public:
		caNode() {
			is_valid_ = 0;
			is_route_ = 0;
		}
		bool is_route() { return is_route_; }
		bool is_valid() { return is_valid_; }
	};
	
	//> This is a route CA node.
	class caRouteNode : public caNode {
	public:
		K key;
		caNode *left, *right;
		pthread_spinlock_t lock_;
	public:
		caRouteNode(K key)
		{
			this->is_route_ = 1;
			this->is_valid_ = 1;
			this->key = key;
			this->left = this->right = NULL;
			pthread_spin_init(&lock_, PTHREAD_PROCESS_SHARED);
		}
		void lock() { pthread_spin_lock(&lock_); }
		void unlock() { pthread_spin_unlock(&lock_); }
	};
	
	//> This is the base CA node, which points to a sequential data structure.
	class caBaseNode : public caNode {
	public:
		Map<K,V> *root; //> this points to the sequential data structure
		long long int lock_statistics;
		pthread_spinlock_t lock_;
	public:
		caBaseNode(K key, Map<K,V> *root)
		{
			this->is_route_ = 0;
			this->is_valid_ = 1;
			this->lock_statistics = 0;
			this->root = root;
//			this->root = new SEQ_DS();
			pthread_spin_init(&lock_, PTHREAD_PROCESS_SHARED);
		}
	
		void lock()
		{
			if (pthread_spin_trylock(&lock_) == 0) {
				//> No contention
				lock_statistics -= STAT_LOCK_SUCC_CONTRIB;
			} else {
				//> Could not lock with trylock(), we have to block
				pthread_spin_lock(&lock_);
				lock_statistics += STAT_LOCK_FAIL_CONTRIB;
			}
		}
		int trylock()
		{
			return pthread_spin_trylock(&lock_);
		}
		void unlock()
		{
			pthread_spin_unlock(&lock_);
		}
	};

	typedef caNode node_t;
	typedef caRouteNode route_node_t;
	typedef caBaseNode base_node_t;

	typedef struct {
		int tid,
		    joins,
		    splits;

		Stack access_path; // used for the range queries
		base_node_t *rquery_bnodes[10000];

		void print() {
			printf("%3d %5d %5d\n", tid, joins, splits);
		}
	} tdata_t;
	tdata_t *tdata_array[88];

private:
	node_t *root;
	pthread_spinlock_t lock;

private:

	base_node_t *get_base_node(route_node_t **parent, route_node_t **gparent, K key)
	{
		route_node_t *p = NULL, *gp = NULL;
		node_t *curr = root;
		route_node_t *rnode;
		while (curr->is_route()) {
			gp = p;
			p = (route_node_t *)curr;
			rnode = (route_node_t *)curr;
			if (key < rnode->key || key == rnode->key) curr = rnode->left;
			else                                       curr = rnode->right;
		}
		*parent = p;
		*gparent = gp;
		return (base_node_t *)curr;
	}

	base_node_t *get_base_node_stack(Stack *stack, K key)
	{
		node_t *curr = root;
		route_node_t *rnode;
		while (curr->is_route()) {
			stack->push(curr);
			rnode = (route_node_t *)curr;
			if (key < rnode->key || key == rnode->key) curr = rnode->left;
			else                                       curr = rnode->right;
		}
		stack->push(curr);
		return (base_node_t *)curr;
	}

	base_node_t *get_leftmost_base(node_t *node, route_node_t **parent,
	                                             route_node_t **gparent)
	{
		route_node_t *p = NULL, *gp = NULL;
		route_node_t *rnode;
		while (node->is_route()) {
			gp = p;
			p = (route_node_t *)node;
			rnode = (route_node_t *)node;
			node = (route_node_t *)rnode->left;
		}
		*parent = p;
		*gparent = gp;
		return (base_node_t *)node;
	}

	base_node_t *get_rightmost_base(node_t *node, route_node_t **parent,
	                                              route_node_t **gparent)
	{
		route_node_t *p = NULL, *gp = NULL;
		route_node_t *rnode;
		while (node->is_route()) {
			gp = p;
			p = (route_node_t *)node;
			rnode = (route_node_t *)node;
			node = (route_node_t *)rnode->right;
		}
		*parent = p;
		*gparent = gp;
		return (base_node_t *)node;
	}

	//> Called with bnode locked
	void split(base_node_t *bnode, route_node_t *parent)
	{
		base_node_t *left_bnode, *right_bnode;
		route_node_t *new_rnode;
	
		if (bnode->root->size() < 10) return;
	
		left_bnode = new base_node_t(-1, NULL);
		right_bnode = new base_node_t(-1, NULL);
		left_bnode->root = (Map<K,V> *)bnode->root->split((void **)&right_bnode->root);

		assert(left_bnode->root != NULL);
	
		bnode->is_valid_ = 0;
	
		new_rnode = new route_node_t(left_bnode->root->max_key());
		new_rnode->left  = (node_t *)left_bnode;
		new_rnode->right = (node_t *)right_bnode;
		if (parent) {
			if (parent->left == (node_t *)bnode) parent->left = (node_t *)new_rnode;
			else                                 parent->right = (node_t *)new_rnode;
		} else {
			root = new_rnode;
		}
	}

	//> Called with bnode locked
	void join(base_node_t *bnode, route_node_t *parent, route_node_t *gparent)
	{
		base_node_t *new_bnode;
		base_node_t *lmost_base, *rmost_base;
		route_node_t *lmparent, *lmgparent; //> Left-most's parent and grandparent
		route_node_t *rmparent, *rmgparent; //> Right-most's parent and grandparent
		node_t *sibling;
	
		if (parent == NULL) return;
	
		new_bnode = new base_node_t(-1, NULL);
	
		if (parent->left == (node_t *)bnode) {
			sibling = parent->right;
	
			lmost_base = get_leftmost_base(sibling, &lmparent, &lmgparent);
			if (lmgparent == NULL) lmgparent = gparent;
			if (lmparent == NULL)  lmparent = parent;
			if (lmgparent == NULL && lmparent != NULL) lmgparent = parent;
	
			//> Try to lock lmost_base and check if valid
			if (lmost_base->trylock() != 0) {
				return;
			} else if (lmost_base->is_valid() == 0) {
				lmost_base->unlock();
				return;
			}
	
			//> Unlink bnode
			if (gparent == NULL) root = parent->right;
			else if (gparent->left == (node_t *)parent) gparent->left = parent->right;
			else if (gparent->right == (node_t *)parent) gparent->right = parent->right;
			bnode->is_valid_ = 0;
			parent->is_valid_ = 0;
	
			new_bnode->root = (Map<K,V> *)bnode->root->join((void *)lmost_base->root);
			if (lmparent == parent) lmparent = gparent; //> lmparent has been spliced out
			if (lmparent == NULL) root = (node_t *)new_bnode;
			else if (lmparent->left == (node_t *)lmost_base)
				lmparent->left = (node_t *)new_bnode;
			else
				lmparent->right = (node_t *)new_bnode;
			lmost_base->is_valid_ = 0;
			lmost_base->unlock();
		} else if (parent->right == (node_t *)bnode) {
			sibling = parent->left;
	
			rmost_base = get_rightmost_base(sibling, &rmparent, &rmgparent);
			if (rmgparent == NULL) rmgparent = gparent;
			if (rmparent == NULL)  rmparent = parent;
			if (rmgparent == NULL && rmparent != NULL) rmgparent = parent;
	
			//> Try to lock rmost_base and check if valid
			if (rmost_base->trylock() != 0) {
				return;
			} else if (rmost_base->is_valid() == 0) {
				rmost_base->unlock();
				return;
			}
	
			//> Unlink bnode
			if (gparent == NULL) root = (node_t *)parent->left;
			else if (gparent->left == (node_t *)parent) gparent->left = parent->left;
			else if (gparent->right == (node_t *)parent) gparent->right = parent->left;
			bnode->is_valid_ = 0;
			parent->is_valid_ = 0;
	
			new_bnode->root = (Map<K,V> *)rmost_base->root->join((void *)bnode->root);
			if (rmparent == parent) rmparent = gparent; //> rmparent has been spliced out
			if (rmparent == NULL) root = (node_t *)new_bnode;
			else if (rmparent->left == (node_t *)rmost_base)
				rmparent->left = (node_t *)new_bnode;
			else
				rmparent->right = (node_t *)new_bnode;
			rmost_base->is_valid_ = 0;
			rmost_base->unlock();
		}
	}

	//> Called with bnode locked
	void adapt_if_needed(base_node_t *bnode, route_node_t *parent,
	                     route_node_t *gparent, tdata_t *tdata)
	{
		if (bnode->lock_statistics > STAT_LOCK_HIGH_CONTENTION_LIMIT) {
			split(bnode, parent);
			bnode->lock_statistics = 0;
//			tdata->splits++;
		} else if (bnode->lock_statistics < STAT_LOCK_LOW_CONTENTION_LIMIT) {
			join(bnode, parent, gparent);
			bnode->lock_statistics = 0;
//			tdata->joins++;
		}
	}

	void print_helper_rec(node_t *node, int depth)
	{
		route_node_t *rnode;
		base_node_t *bnode;
	
		if (node->is_route()) {
			rnode = (route_node_t *)node;
			print_helper_rec(rnode->right, depth+1);
			for (int i=0; i < depth; i++) printf("-");
			printf("-> [ROUTE] ");
			printf("%llu\n", rnode->key);
			print_helper_rec(rnode->left, depth+1);
		} else {
			bnode = (base_node_t *)node;
			for (int i=0; i < depth; i++) printf("-");
			printf("-> [BASE] (size: %llu min: %llu max: %llu)\n",
			        bnode->root->size(), bnode->root->min_key(),
			        bnode->root->max_key());
		}
	}

	int bst_violations;
	int total_nodes, route_nodes, base_nodes, invalid_nodes;
	int total_keys, base_keys;
	int max_depth, min_depth;
	int max_seq_ds_size, min_seq_ds_size;
	int invalid_seq_data_structures;

	void validate_rec(node_t *n, K min, K max, int depth)
	{
		route_node_t *rnode;
		base_node_t *bnode;
		int sz;
	
		total_nodes++;
		total_keys++;
	
		if (n->is_route()) {
			rnode = (route_node_t *)n;
			route_nodes++;
			invalid_nodes += (rnode->is_valid() == 0);
			validate_rec(rnode->left, min, rnode->key, depth+1);
			validate_rec(rnode->right, rnode->key, max, depth+1);
		} else {
			bnode = (base_node_t *)n;
			base_nodes++;
			invalid_nodes += (bnode->is_valid() == 0);
			invalid_seq_data_structures += !bnode->root->validate(false);
			if (!bnode->root->is_empty() && bnode->root->max_key() > max) bst_violations++;
			if (!bnode->root->is_empty() && bnode->root->min_key() < min) bst_violations++;
			if (depth < min_depth) min_depth = depth;
			if (depth > max_depth) max_depth = depth;
			sz = bnode->root->size();
			base_keys += sz;
			if (sz < min_seq_ds_size) min_seq_ds_size = sz;
			if (sz > max_seq_ds_size) max_seq_ds_size = sz;
		}
	}

public:

	void print()
	{
		print_helper_rec(root, 0);
	}

	bool do_contains(const int tid, const K& key, tdata_t *tdata)
	{
		int ret = 0;
		base_node_t *bnode;
		route_node_t *parent, *gparent;
	
		while (1) {
			bnode = get_base_node(&parent, &gparent, key);
			bnode->lock();
			if (!bnode->is_valid()) {
				bnode->unlock();
				continue;
			}
			ret = bnode->root->contains(tid, key);
			adapt_if_needed(bnode, parent, gparent, tdata);
			bnode->unlock();
			return ret;
		}
	}

	/**
	 * For a range query of [key1, key2] returns (locked) all the base nodes involved.
	 * Returns the number of base nodes, or -1 if some of the base nodes was invalid.
	 */
	int rquery_get_base_nodes(K key1, K key2, tdata_t *tdata)
	{
		base_node_t *bnode;
		route_node_t *rnode;
		node_t *curr, *prev;
		int nbase_nodes = 0;
	
		tdata->access_path.reset();
	
		get_base_node_stack(&tdata->access_path, key1);
		curr = (node_t *)tdata->access_path.pop();
		prev = curr;
	
		while (curr != NULL) {
			if (curr->is_route()) {
				rnode = (route_node_t *)curr;
				if (!rnode->is_valid()) goto out_with_valid_error;
				if (prev != rnode->left && prev != rnode->right) {
					//> None of the two children have been examined, go to left
					curr = rnode->left;
					tdata->access_path.push(rnode);
				} else if (rnode->left == prev) {
					//> Previous examined node was its left child, go to right
					curr = rnode->right;
					tdata->access_path.push(rnode);
				} else {
					//> Both children have already been examined, go upwards
					prev = curr;
					curr = (node_t *)tdata->access_path.pop();
				}
			} else {
				bnode = (base_node_t *)curr;
				if (bnode->trylock()) {
					bnode->lock_statistics += STAT_LOCK_FAIL_CONTRIB;
					goto out_with_valid_error;
				}
				bnode->lock_statistics -= STAT_LOCK_SUCC_CONTRIB;
				tdata->rquery_bnodes[nbase_nodes++] = bnode;
				if (!bnode->is_valid()) goto out_with_valid_error;
				if (!bnode->root->is_empty() && !(bnode->root->max_key() < key2)) break;
				route_node_t *parent, *gparent;
				parent = (route_node_t *)tdata->access_path.pop();
				gparent = (route_node_t *)tdata->access_path.pop();
				adapt_if_needed(bnode, parent, gparent, tdata);
				tdata->access_path.push(gparent);
				tdata->access_path.push(parent);
	
				prev = curr;
				curr = (node_t *)tdata->access_path.pop();
			}
		}
		return nbase_nodes;
	
	out_with_valid_error:
		for (int i=0; i < nbase_nodes; i++)
			tdata->rquery_bnodes[i]->unlock();
		return -1;
	}

	bool do_rangeQuery(const int tid, const K& low, const K& hi,
	                   std::vector<std::pair<K,V>> kv_pairs, tdata_t *tdata)
	{
		int nbase_nodes;
		int nkeys = 0;

		do {
			nbase_nodes = rquery_get_base_nodes(low, hi, tdata);
		} while (nbase_nodes == -1);

		//> Get appropriate keys from each base node and unlock it
		for (int i=0; i < nbase_nodes; i++) {
			nkeys += tdata->rquery_bnodes[i]->root->rangeQuery(tid, low, hi, kv_pairs);
			tdata->rquery_bnodes[i]->unlock();
		}

		return nkeys;
	}

	V do_insert(const int tid, const K& key, const V& value, tdata_t *tdata)
	{
		V ret;
		base_node_t *bnode;
		route_node_t *parent, *gparent;
	
		while (1) {
			bnode = get_base_node(&parent, &gparent, key);
			bnode->lock();
			if (!bnode->is_valid()) {
				bnode->unlock();
				continue;
			}
			ret = bnode->root->insert(tid, key, value);
			adapt_if_needed(bnode, parent, gparent, tdata);
			bnode->unlock();
			return ret;
		}
	}

	std::pair<V,bool> do_remove(const int tid, const K& key, tdata_t *tdata)
	{
		std::pair<V,bool> ret;
		base_node_t *bnode;
		route_node_t *parent, *gparent;
	
		while (1) {
			bnode = get_base_node(&parent, &gparent, key);
			bnode->lock();
			if (!bnode->is_valid()) {
				bnode->unlock();
				continue;
			}
			ret = bnode->root->remove(tid, key);
			adapt_if_needed(bnode, parent, gparent, tdata);
			bnode->unlock();
			return ret;
		}
	}

	bool do_validate()
	{
		int check_bst = 0;
		bst_violations = 0;
		total_nodes = route_nodes = base_nodes = invalid_nodes = 0;
		total_keys = base_keys = 0;
		min_depth = 100000;
		max_depth = -1;
		min_seq_ds_size = 9999999;
		max_seq_ds_size = -1;
		invalid_seq_data_structures = 0;
	
		if (root) validate_rec(root, 0, 99999999999, 0);
	
		check_bst = (bst_violations == 0) && (invalid_seq_data_structures == 0);
	
		printf("Validation:\n");
		printf("=======================\n");
		printf("  BST Violation: %s\n",
		       check_bst ? "No [OK]" : "Yes [ERROR]");
		printf("  Invalid nodes: %d %s\n", invalid_nodes,
		       invalid_nodes == 0 ? "[OK]" : "[ERROR]");
		printf("  Invalid Base Sequential Data Structures: %d %s\n",
		       invalid_seq_data_structures,
		       invalid_seq_data_structures == 0 ? "[OK]" : "[ERROR]");
		printf("  Tree size: %8d ( %8d route / %8d base )\n",
		       total_nodes, route_nodes, base_nodes);
		printf("  Number of keys: %8d total / %8d in base nodes\n", total_keys,
		                                                            base_keys);
		printf("  Depth (min/max): %d / %d\n", min_depth, max_depth);
		printf("  Sequential Data Structures Sizes (min/max): %d / %d\n",
		          min_seq_ds_size, max_seq_ds_size);
		printf("\n");
	
		return check_bst;
	}

	long long get_key_sum_rec(node_t *n)
	{
		route_node_t *rnode;
		base_node_t *bnode;

		if (n->is_route()) {
			rnode = (route_node_t *)n;
			return get_key_sum_rec(rnode->left) + get_key_sum_rec(rnode->right);
		} else {
			bnode = (base_node_t *)n;
			return bnode->root->get_key_sum();
		}
	}

public:

	//>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//> This is the public interface the benchmarks use
	//>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

	ca_locks(const K _NO_KEY, const V _NO_VALUE, const int numProcesses, Map<K,V> *seq_ds)
	{
		pthread_spin_init(&lock, PTHREAD_PROCESS_SHARED);
		root = (node_t *)new base_node_t(-1, seq_ds);
		seq_ds_name = seq_ds->name();
	}
	~ca_locks()
	{
	}

	void initThread(const int tid) {
		tdata_array[tid] = new tdata_t();
		tdata_array[tid]->tid = tid;
	}

	void deinitThread(const int tid) {
		tdata_t *tdata = tdata_array[tid];
		tdata->print();
	}

	const V insert(const int tid, const K& key, const V& val)
	{
		return insertIfAbsent(tid, key, val);
	}

	const V insertIfAbsent(const int tid, const K& key, const V& val)
	{
		tdata_t *tdata = tdata_array[tid];
		return do_insert(tid, key, val, tdata);
	}

	const std::pair<V,bool> remove(const int tid, const K& key)
	{
		tdata_t *tdata = tdata_array[tid];
		return do_remove(tid, key, tdata);
	}

	bool contains(const int tid, const K& key)
	{
		tdata_t *tdata = tdata_array[tid];
		return do_contains(tid, key, tdata);
	}

	const std::pair<V,bool> find(const int tid, const K& key)
	{
		return std::pair<V,bool>(NULL, false);
	}

	int rangeQuery(const int tid, const K& low, const K& hi,
	                     std::vector<std::pair<K,V>> kv_pairs)
	{
		tdata_t *tdata = tdata_array[tid];
		return do_rangeQuery(tid, low, hi, kv_pairs, tdata);
	}

	bool validate(const long long keysum, const bool checkkeysum)
	{
		return do_validate();
	}

	bool validate()
	{
		return do_validate();
	}

	char *name()
	{
		char *seqds= seq_ds_name;
		char *name = new char[60];
		sprintf(name, "%s (%s)", seqds, "Contention-adaptive");
		return name;
	}

	long long debugKeySum()
	{
		return get_key_sum_rec(root);
	}
	long long getSize()
	{
		return 0;
	}
};
