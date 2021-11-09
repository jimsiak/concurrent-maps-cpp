#pragma once

#include <iostream>
#include <assert.h>
#include <pthread.h>

#include "Stack.h"
#include "../map_if.h"

template<typename K>
class TreapNode {
private:
	bool is_internal_;

public:
	bool is_internal() { return is_internal_; };
	void set_internal(bool yes) { is_internal_ = yes; }
};

//> 'left' and 'right' point either to internal or external type of nodes
template<typename K>
class TreapNodeInternal : TreapNode<K> {
public:
	K key;
	unsigned long long weight;
	TreapNode<K> *left,
	             *right;

public:
	TreapNodeInternal(K key_)
	{
		this->set_internal(true);
		key = key_;
		weight = 0;
		left = right = NULL;
	}

	void print()
	{
		std::cout << "I: [key: " << key << ", weight: " << weight << "]\n";
	}
};

template<typename K, typename V, int DEGREE>
class TreapNodeExternal : TreapNode<K> {
private:
	typedef TreapNodeExternal<K, V, DEGREE> node_external_t;

public:
	int nr_keys;
	K keys[DEGREE];
	V values[DEGREE];

public:
	TreapNodeExternal(const K& key_, const V& value_)
	{
		this->set_internal(false);
		keys[0] = key_;
		values[0] = value_;
		nr_keys = 1;
	}

	void print()
	{
		std::cout << "E: [keys: ";
		for (int i=0; i < nr_keys; i++)
			std::cout << keys[i] << "| ";
		std::cout << "]\n";
	}

	/**
	 * Validates the following:
	 * 1. Keys inside node are sorted.
	 * 2. Keys inside node are higher than min and less than or equal to max.
	 **/
	bool validate(K min, K max)
	{
		for (int i=1; i < nr_keys; i++)
			if (keys[i] < keys[i-1])
				return false;
		if (keys[0] < min || max < keys[nr_keys-1])
			return false;
		return true;
	}

	int is_full() { return nr_keys >= DEGREE; }
	int is_empty() { return nr_keys == 0; }

	int index_of(const K key)
	{
		for (int i=0; i < nr_keys; i++) {
			if (keys[i] == key) return i;
			if (key < keys[i]) return -1;
		}
		return -1;
	}

	//> This version of "index_of" is used in range queries.
	//> Instead of returning -1 when the key is not found in the node,
	//> return the index where the first larger key is found
	int index_of_equal_or_larger(const K key)
	{
		for (int i=0; i < nr_keys; i++) {
			if (keys[i] == key) return i;
			if (key < keys[i]) return i;
		}
		return nr_keys;
	}

	node_external_t *split()
	{
		node_external_t *new_external = new node_external_t(keys[0], NULL);
	
		for (int i=nr_keys/2; i < nr_keys; i++) {
			new_external->keys[i - nr_keys/2] = keys[i];
			new_external->values[i - nr_keys/2] = values[i];
		}
		new_external->nr_keys = nr_keys - nr_keys / 2;
		nr_keys = nr_keys / 2;
		return new_external;
	}

	void insert(const K& key, const V& value)
	{
		assert(nr_keys < DEGREE);

		int i = nr_keys;
		while (i-1 >= 0 && key < keys[i-1]) {
			keys[i] = keys[i-1];
			values[i] = values[i-1];
			i--;
		}
		keys[i] = key;
		values[i] = value;
		nr_keys++;
	}

	void delete_at(int index)
	{
		assert(index >= 0 && index < DEGREE);
		for (int i=index; i < nr_keys; i++) {
			keys[i] = keys[i+1];
			values[i] = values[i+1];
		}
		nr_keys--;
	}

	long long get_key_sum()
	{
		long long sum = 0;
		for (int i=0; i < nr_keys; i++)
			sum += keys[i];
		return sum;
	}
};


#define TREAP_TEMPLATE_DEFAULT template<typename K, typename V, int DEGREE = 64>
#define TREAP_TEMPLATE template<typename K, typename V, int DEGREE>
#define TREAP Treap<K,V,DEGREE>

TREAP_TEMPLATE_DEFAULT
class Treap : public Map<K,V> {
private:

	typedef Treap<K, V, DEGREE> treap_t;
	typedef TreapNode<K> node_t;
	typedef TreapNodeInternal<K> node_internal_t;
	typedef TreapNodeExternal<K, V, DEGREE> node_external_t;

	node_t *root;

public:
	Treap(const K _NO_KEY, const V _NO_VALUE, const int numProcesses)
	  : Map<K,V>(_NO_KEY, _NO_VALUE)
	{
		root = NULL;
	}

	void print()
	{
		if (!root) std::cout << "EMPTY\n";
		else print_rec(root, 0);
	}

public:
	/**
	 * The Map public interface.
	 **/

	void initThread(const int tid) {};
	void deinitThread(const int tid) {};

	bool contains(const int tid, const K& key);
	const std::pair<V,bool> find(const int tid, const K& key);
	int rangeQuery(const int tid, const K& key1, const K& key2,
	               std::vector<std::pair<K,V>> kv_pairs);

	const V insert(const int tid, const K& key, const V& val);
	const V insertIfAbsent(const int tid, const K& key, const V& val);
	const std::pair<V, bool> remove(const int tid, const K& key);

	bool validate() { return validate(true); }

	char *name() { return (char *)"Treap"; }

public:
	bool validate(bool print);

	unsigned long long size() { return size_rec(root); }
	bool is_empty() { return root == NULL; }
	long long get_key_sum() { return get_key_sum_rec(root); }

private:

	void print_rec(node_t *node, int level)
	{
		if (node->is_internal()) {
			node_internal_t *internal = (node_internal_t *)node;
			print_rec(internal->right, level+1);
			for (int i=0; i < level; i++) printf("-"); printf("> ");
			internal->print();
			print_rec(internal->left, level+1);
		} else {
			node_external_t *external = (node_external_t *)node;
			for (int i=0; i < level; i++) printf("-"); printf("> ");
			external->print();
		}
	}

	unsigned long long size_rec(node_t *root)
	{
		node_internal_t *internal;
		node_external_t *external;
	
		if (!root) return 0;
	
		if (root->is_internal()) {
			internal = (node_internal_t *)root;
			return size_rec(internal->left) + size_rec(internal->right);
		} else {
			external = (node_external_t *)root;
			return external->nr_keys;
		}
	}

	long long get_key_sum_rec(node_t *n)
	{
		node_internal_t *internal;
		node_external_t *external;
	
		if (!n) return 0;

		if (n->is_internal()) {
			internal = (node_internal_t *)n;
			return get_key_sum_rec(internal->left) + get_key_sum_rec(internal->right);
		} else {
			external = (node_external_t *)n;
			return external->get_key_sum();
		}
	}

	node_external_t *traverse(K key)
	{
		node_t *curr = root;
		node_internal_t *internal;
	
		while (curr != NULL && curr->is_internal()) {
			internal = (node_internal_t *)curr;
			curr = (internal->key < key) ? internal->right : internal->left;
		}
		return (node_external_t *)curr;
	}

	void traverse_with_stack(K key, Stack *stack)
	{
		node_t *curr = root;
		node_internal_t *internal;
	
		stack->reset();
		while (curr != NULL && curr->is_internal()) {
			internal = (node_internal_t *)curr;
			stack->push(internal);
			curr = (internal->key < key) ? internal->right : internal->left;
		}
		//> Also add the final external node in the stack
		if (curr != NULL) stack->push(curr);
	}

	void rebalance(Stack *stack)
	{
		node_internal_t *curr, *parent, *gparent;
	
		while (1) {
			curr = (node_internal_t *)stack->pop();
			parent = (node_internal_t *)stack->pop();
			if (curr == NULL || parent == NULL || curr->weight <= parent->weight) break;
	
			if (curr == (node_internal_t *)parent->left) {
				//> Right rotation
				parent->left = curr->right;
				curr->right = (node_t *)parent;
			} else {
				//> Left rotation
				parent->right = curr->left;
				curr->left = (node_t *)parent;
			}
	
			gparent = (node_internal_t *)stack->pop();
			if (gparent == NULL)              root = (node_t *)curr;
			else if (parent == (node_internal_t *)gparent->left)
				gparent->left = (node_t *)curr;
			else
				gparent->right = (node_t *)curr;
	
			stack->push(gparent);
			stack->push(curr);
		}
	}

	void _do_insert(node_external_t *external, Stack *stack,
	                const K& key, const V& value)
	{
		node_external_t *new_external;
		node_internal_t *new_internal, *parent;
	
		//> 1. External node has space for one more key
		if (!external->is_full()) {
			external->insert(key, value);
			return;
		}
	
		//> 2. No space left in the external node, need to split
		new_external = external->split();
		if (key < external->keys[external->nr_keys-1])
			external->insert(key, value);
		else
			new_external->insert(key, value);
	
		new_internal = new node_internal_t(external->keys[external->nr_keys-1]);
		new_internal->left = (node_t *)external;
		new_internal->right = (node_t *)new_external;
	
		parent = (node_internal_t *)stack->pop();
		if (parent == NULL) {
			root = (node_t *)new_internal;
		} else {
			if (new_internal->key < parent->key) parent->left = (node_t *)new_internal;
			else                                 parent->right = (node_t *)new_internal;
	
			stack->push(parent);
			stack->push(new_internal);
			rebalance(stack);
		}
	}

	void _do_delete(node_external_t *external, Stack *stack, int key_index)
	{
		node_internal_t *internal, *internal_parent;
		node_external_t *sibling;
	
		external->delete_at(key_index);
	
		//> 3. External node is now empty
		if (external->is_empty()) {
			internal = (node_internal_t *)stack->pop();
			if (internal == NULL) {
				root = NULL;
			} else {
				sibling = (external == (node_external_t *)internal->left) ?
				                          (node_external_t *)internal->right :
				                          (node_external_t *)internal->left;
				internal_parent = (node_internal_t *)stack->pop();
				if (internal_parent == NULL) {
					root = (node_t *)sibling;
				} else {
					if (internal == (node_internal_t *)internal_parent->left)
						internal_parent->left = (node_t *)sibling;
					else
						internal_parent->right = (node_t *)sibling;
				}
			}
		}
	
	}

	int bst_violations, heap_violations;
	int total_nodes, internal_nodes, external_nodes;
	int total_keys, leaf_keys;
	int max_depth, min_depth;

	void validate_rec(node_t *n, K min, K max,
	                  unsigned long long max_priority, int depth)
	{
		node_internal_t *internal;
		node_external_t *external;
		total_nodes++;
	
		if (n->is_internal()) {
			internal_nodes++;
			total_keys++;
	
			internal = (node_internal_t *)n;
			if (internal->weight > max_priority)
				heap_violations++;
			if (internal->key < min || max < internal->key) {
				printf("internal->key: %llu min: %llu max: %llu\n", internal->key, min, max);
				bst_violations++;
			}
			validate_rec(internal->left, min, internal->key, internal->weight, depth+1);
			validate_rec(internal->right, internal->key, max, internal->weight, depth+1);
		} else {
			external_nodes++;
			
			external = (node_external_t *)n;
			if (!external->validate(min, max)) {
				printf("SKATAAAA\n");
				bst_violations++;
			}
			if (depth < min_depth) min_depth = depth;
			if (depth > max_depth) max_depth = depth;
			leaf_keys += external->nr_keys;
			total_keys += external->nr_keys;
		}
	}

public:
	/**
	 * The public methods that make this class usable with contention-adaptive
	 * synchronization.
	 **/

	const K& max_key()
	{
		node_internal_t *internal;
		node_external_t *external;
		node_t *curr = root;
	
		while (curr->is_internal()) {
			internal = (node_internal_t *)curr;
			curr = internal->right;
		}
		external = (node_external_t *)curr;
		return external->keys[external->nr_keys-1];
	}
	
	const K& min_key()
	{
		node_internal_t *internal;
		node_external_t *external;
		node_t *curr = root;
	
		while (curr->is_internal()) {
			internal = (node_internal_t *)curr;
			curr = internal->left;
		}
		external = (node_external_t *)curr;
		return external->keys[0];
	}
	
	//> Splits the treap in two treaps.
	//> The left part is returned and the right part is put in *right_part
	void *split(void **_right_part)
	{
		treap_t **right_part = (treap_t **)_right_part;
		int i;
		treap_t *right_treap;
		node_internal_t *internal;
		node_external_t *external;
	
		*right_part = NULL;
		if (!root) return NULL;
	
		right_treap = new treap_t(this->INF_KEY, this->NO_VALUE, 88);
		if (root->is_internal()) {
			internal = (node_internal_t *)root;
			right_treap->root = internal->right;
			root = internal->left;
		} else {
			external = (node_external_t *)root;
			right_treap->root = (node_t *)external->split();
		}
		*right_part = right_treap;
		return (void *)this;
	}
	
	//> Joins 'this' and treap_right and returns the joint treap
	void *join(void *_treap_right)
	{
		treap_t *treap_right = (treap_t *)_treap_right;
		treap_t *treap_left = this;
		node_internal_t *new_internal;
	
		if (!treap_left->root) return treap_right;
		else if (!treap_right->root) return treap_left;
	
		new_internal = new node_internal_t(treap_left->max_key());
		new_internal->left = treap_left->root;
		new_internal->right = treap_right->root;
	
		treap_left->root = (node_t *)new_internal;
		return (void *)treap_left;
	}
};

TREAP_TEMPLATE
bool TREAP::contains(const int tid, const K& key)
{
	node_external_t *external = traverse(key);
	return ( (external != NULL) && (external->index_of(key) != -1) );
}

TREAP_TEMPLATE
const std::pair<V,bool> TREAP::find(const int tid, const K& key)
{
	node_external_t *external = traverse(key);
	int index = external->index_of(key);
	if (external == NULL || index == -1) return std::pair<V,bool>(this->NO_VALUE, false);
	return std::pair<V,bool>(external->values[index], true);
}

TREAP_TEMPLATE
int TREAP::rangeQuery(const int tid, const K& key1, const K& key2,
                      std::vector<std::pair<K,V>> kv_pairs)
{
	node_t *curr, *prev = NULL;
	node_external_t *external;
	node_internal_t *internal;
	int stack_sz, key_index, nkeys;
	Stack stack;

	traverse_with_stack(key1, &stack);
	stack_sz = stack.size();
	if (stack_sz == 0) return 0;

	nkeys = 0;
	while (1) {
		curr = (node_t *)stack.pop();
		if (!curr) {
			break;
		} if (curr->is_internal()) {
			internal = (node_internal_t *)curr;
			if (!prev) {
				//> New internal node to visit
				stack.push(internal);
				stack.push(internal->left);
			} else if (prev == internal->left) {
				//> Already visited node's left child
				stack.push(internal);
				stack.push(internal->right);
				prev = NULL;
			} else if (prev == internal->right) {
				//> Already visited node's both children
				prev = (node_t *)internal;
			}
		} else {
			external = (node_external_t *)curr;
			key_index = external->index_of_equal_or_larger(key1);
			while (key_index < external->nr_keys &&
			       (external->keys[key_index] == key1 ||
			        key1 < external->keys[key_index]) &&
			       (external->keys[key_index] == key2 ||
			        external->keys[key_index] < key2)) {
				kv_pairs.push_back(std::pair<K,V>(external->keys[key_index],
				                                  external->values[key_index++]));
			}
			if (key_index < external->nr_keys)
				break;
			prev = (node_t *)external;
		}
	}

	return nkeys;
}

TREAP_TEMPLATE
const V TREAP::insert(const int tid, const K& key, const V& val)
{
	return insertIfAbsent(tid, key, val);
}

TREAP_TEMPLATE
const V TREAP::insertIfAbsent(const int tid, const K& key, const V& val)
{
	node_external_t *external;
	int stack_sz, key_index;
	Stack stack;

	traverse_with_stack(key, &stack);
	stack_sz = stack.size();

	//> 1. Empty treap
	if (stack_sz == 0) {
		root = (node_t *)new node_external_t(key, val);
		return this->NO_VALUE;
	}
	
	external = (node_external_t *)stack.pop();
	key_index = external->index_of(key);

	//> 2. Key already in the tree
	if (key_index != -1) return external->values[key_index];

	//> 3. Key not in the tree, insert it
	_do_insert(external, &stack, key, val);
	return this->NO_VALUE;
}

TREAP_TEMPLATE
const std::pair<V, bool> TREAP::remove(const int tid, const K& key)
{
	node_external_t *external;
	int stack_sz, key_index;
	Stack stack;

	traverse_with_stack(key, &stack);
	stack_sz = stack.size();

	//> 1. Empty treap
	if (stack_sz == 0) return std::pair<V,bool>(this->NO_VALUE, false);

	external = (node_external_t *)stack.pop();
	key_index = external->index_of(key);

	//> 2. Key not in the tree
	if (key_index == -1) return std::pair<V,bool>(this->NO_VALUE, false);

	const V del_val = external->values[key_index];
	_do_delete(external, &stack, key_index);
	return std::pair<V,bool>(del_val, true);
}

TREAP_TEMPLATE
bool TREAP::validate(bool print)
{
	int check_bst = 0, check_heap = 0;
	bst_violations = 0;
	heap_violations = 0;
	total_nodes = 0;
	internal_nodes = 0;
	external_nodes = 0;
	total_keys = 0;
	leaf_keys = 0;
	min_depth = 100000;
	max_depth = -1;

	if (root) validate_rec(root, 0, this->INF_KEY, 9999999, 0);

	check_bst = (bst_violations == 0);
	check_heap = (heap_violations == 0);

	if (print) {
		printf("Validation:\n");
		printf("=======================\n");
		printf("  BST Violation: %s\n",
		       check_bst ? "No [OK]" : "Yes [ERROR]");
		printf("  HEAP Violation: %s\n",
		       check_heap ? "No [OK]" : "Yes [ERROR]");
		printf("  Tree size: %8d ( %8d internal / %8d external )\n",
		       total_nodes, internal_nodes, external_nodes);
		printf("  Number of keys: %8d total / %8d in leaves\n", total_keys, leaf_keys);
		printf("  Depth (min/max): %d / %d\n", min_depth, max_depth);
		printf("\n");
	}

	return check_bst && check_heap;
}
