/**
 * An (a,b)-tree.
 **/

#pragma once

#include "../map_if.h"
#include "Log.h"

#define ABTREE_DEGREE_MAX 16
#define ABTREE_DEGREE_MIN 8
#define ABTREE_MAX_HEIGHT 20

typedef int key_t;
#define ABTREE_MAX_KEY 999999999

template <typename K, typename V>
class abtree : public Map<K,V> {
public:
	abtree(const K _NO_KEY, const V _NO_VALUE, const int numProcesses)
	{
		root = NULL;
	}

	void initThread(const int tid) {};
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
	char *name() { return "(a,b)-tree"; }

	void print() { print_helper(); };
//	unsigned long long size() { return size_rec(root); };

private:

	struct node_t {
		bool leaf, marked, tag;
	
		int no_keys;
		K keys[ABTREE_DEGREE_MAX];
		__attribute__((aligned(16))) void *children[ABTREE_DEGREE_MAX + 1];

		node_t (bool leaf) {
			this->no_keys = 0;
			this->leaf = leaf;
		}

		int search(const K& key)
		{
			int i = 0;
			while (i < no_keys && key > keys[i]) i++;
			return i;
		}
		
		node_t *get_child(const K& key)
		{
			int index = search(key);
			if (index < no_keys && keys[index] == key) index++;
			return children[index];
		}

		void delete_index(int index)
		{
			int i;
			assert(index < no_keys);
			for (i=index+1; i < no_keys; i++) {
				keys[i-1] = keys[i];
				children[i] = children[i+1];
			}
			no_keys--;
		}
		
		/**
		 * Insert 'key' in position 'index' of node 'this'.
		 * 'ptr' is either a pointer to data record in the case of a leaf 
		 * or a pointer to a child node_t in the case of an internal node.
		 **/
		void insert_index(int index, const K& key, void *ptr)
		{
			int i;
		
			for (i=no_keys; i > index; i--) {
				keys[i] = keys[i-1];
				children[i+1] = children[i];
			}
			keys[index] = key;
			children[index+1] = ptr;
		
			no_keys++;
		}

		K& get_max_key()
		{
			K& max = keys[0];
			for (int i=1; i < no_keys; i++)
				if (keys[i] > max) max = keys[i];
			return max;
		}

		void print()
		{
			printf("abtree_node: [");
		
			for (int i=0; i < no_keys; i++)
				printf(" %llu |", keys[i]);
			printf("]");
			printf("%s - %s\n", leaf ? " LEAF" : "", tag ? " TAGGED" : "");
		}
	};

	node_t *root;

private:

	int lookup_helper(const K& key)
	{
		int index;
		node_t *n = root;
	
		//> Empty tree.
		if (!n) return 0;
	
		while (!n->leaf) {
			index = n->search(key);
			if (index < n->no_keys && n->keys[index] == key) index++;
			n = (node_t *)n->children[index];
		}
		index = n->search(key);
		return (n->keys[index] == key);
	}

	void traverse_with_stack(const K& key,
	                         node_t **node_stack, int *node_stack_indexes,
	                         int *node_stack_top)
	{
		int index;
		node_t *n;
	
		*node_stack_top = -1;
		n = root;
		if (!n) return;
	
		while (!n->leaf) {
			index = n->search(key);
			if (index < n->no_keys && n->keys[index] == key) index++;
			node_stack[++(*node_stack_top)] = n;
			node_stack_indexes[*node_stack_top] = index;
			n = (node_t*)n->children[index];
		}
		index = n->search(key);
		node_stack[++(*node_stack_top)] = n;
		node_stack_indexes[*node_stack_top] = index;
	}

	void join_parent_with_child(node_t *p, int pindex, node_t *l)
	{
		int new_no_keys = p->no_keys + l->no_keys;
		int i;
	
		//> copy keys first
		int first_key_to_shift = pindex;
		int last_key_to_shift = p->no_keys - 1;
		int index = new_no_keys - 1;
		for (i=last_key_to_shift; i >= first_key_to_shift; i--)
			p->keys[index--] = p->keys[i];
		for (i=0; i < l->no_keys; i++)
			p->keys[i + first_key_to_shift] = l->keys[i];
	
		//> copy pointers then
		int first_ptr_to_shift = pindex;
		int last_ptr_to_shift = p->no_keys;
		index = new_no_keys;
		for (i=last_ptr_to_shift; i >= first_ptr_to_shift; i--)
			p->children[index--] = p->children[i];
		for (i=0; i <= l->no_keys; i++)
			p->children[i + first_ptr_to_shift] = l->children[i];
	
		p->no_keys = new_no_keys;
	}

	void split_parent_and_child(node_t *p, int pindex, node_t *l)
	{
		int i, k1 = 0, k2 = 0;
		K keys[ABTREE_DEGREE_MAX * 2];
		void *ptrs[ABTREE_DEGREE_MAX * 2 + 1];
	
		//> Get all keys
		for (i=0; i < pindex; i++)          keys[k1++] = p->keys[i];
		for (i=0; i < l->no_keys; i++)      keys[k1++] = l->keys[i];
		for (i=pindex; i < p->no_keys; i++) keys[k1++] = p->keys[i];
		//> Get all pointers
		for (i=0; i < pindex; i++)            ptrs[k2++] = p->children[i];
		for (i=0; i <= l->no_keys; i++)        ptrs[k2++] = l->children[i];
		for (i=pindex+1; i <= p->no_keys; i++) ptrs[k2++] = p->children[i];
	
		k1 = k2 = 0;
		int sz = p->no_keys + l->no_keys;
		int leftsz = sz / 2;
		int rightsz = sz - leftsz - 1;
	
		//> Create new left node
		node_t *new_left = new node_t(0);
		for (i=0; i < leftsz; i++) {
			new_left->keys[i] = keys[k1++];
			new_left->children[i] = ptrs[k2++];
		}
		new_left->children[leftsz] = ptrs[k2++];
		new_left->tag = 0;
		new_left->no_keys = leftsz;
	
		//> Fix the parent
		p->keys[0] = keys[k1++];
		p->children[0] = (void*)new_left;
		p->children[1] = (void*)l;
		// FIXME tag parent here?? or outside this function??
		p->no_keys = 1;
	
		//> Fix the old l node which now becomes the right node
		for (i=0; i < rightsz; i++) {
			l->keys[i] = keys[k1++];
			l->children[i] = ptrs[k2++];
		}
		l->children[rightsz] = ptrs[k2++];
		l->tag = 0;
		l->no_keys = rightsz;
	}

	void join_siblings(node_t *p, node_t *l, node_t *s, int lindex, int sindex)
	{
		int left_index, right_index;
		node_t *left, *right;
		int i, k1, k2;
	
		left_index  = lindex < sindex ? lindex : sindex;
		right_index = lindex < sindex ? sindex : lindex;
		left  = (node_t*)p->children[left_index];
		right = (node_t*)p->children[right_index];
	
		//> Move all keys to the left node
		k1 = left->no_keys;
		k2 = left->no_keys + 1;
		if (!left->leaf) left->keys[k1++] = p->keys[left_index];
		for (i=0; i < right->no_keys; i++)
			left->keys[k1++] = right->keys[i];
		for (i = (left->leaf) ? 1 : 0; i < right->no_keys; i++)
			left->children[k2++] = right->children[i];
		left->children[k2++] = right->children[right->no_keys];
		left->tag = 0;
		left->no_keys = k1;
		
		//> Fix the parent
		for (i=left_index + 1; i < p->no_keys; i++) {
			p->keys[i-1] = p->keys[i];
			p->children[i] = p->children[i+1];
		}
		p->tag = 0;
		p->no_keys--;
	}

	void redistribute_sibling_keys(node_t *p, node_t *l, node_t *s,
	                               int lindex, int sindex)
	{
		K keys[ABTREE_DEGREE_MAX * 2];
		void *ptrs[ABTREE_DEGREE_MAX * 2 + 1];
		int left_index, right_index;
		node_t *left, *right;
		int i, k1 = 0, k2 = 0, total_keys, left_keys, right_keys;
	
		left_index  = lindex < sindex ? lindex : sindex;
		right_index = lindex < sindex ? sindex : lindex;
		left  = (node_t*)p->children[left_index];
		right = (node_t*)p->children[right_index];
	
		//> Gather all keys in keys array
		for (i=0; i < left->no_keys; i++) {
			keys[k1++] = left->keys[i];
			ptrs[k2++] = left->children[i];
		}
		ptrs[k2++] = left->children[left->no_keys];
		if (!left->leaf) keys[k1++] = p->keys[left_index];
		for (i=0; i < right->no_keys; i++)
			keys[k1++] = right->keys[i];
		for (i = (left->leaf) ? 1 : 0; i < right->no_keys; i++)
			ptrs[k2++] = right->children[i];
		ptrs[k2++] = right->children[right->no_keys];
	
		//> Calculate new number of keys in left and right
		total_keys = k1;
		left_keys = k1 / 2;
		right_keys = total_keys - left_keys;
		if (!left->leaf) right_keys--; // If not leaf one key goes to parent
	
		//> Fix left
		k1 = k2 = 0;
		for (i=0; i < left_keys; i++) {
			left->keys[i] = keys[k1++];
			left->children[i] = ptrs[k2++];
		}
		left->children[left_keys] = ptrs[k2++];
		left->no_keys = left_keys;
	
		//> Fix parent
		p->keys[left_index] = keys[k1];
		if (!left->leaf) k1++; 
	
		//> Fix right
		for (i=0; i < right_keys; i++)
			right->keys[i] = keys[k1++];
		for (i = (left->leaf) ? 1 : 0; i < right_keys; i++)
			right->children[i] = ptrs[k2++];
		right->children[right_keys] = ptrs[k2++];
		right->no_keys = right_keys;
	}

	void rebalance(node_t **node_stack, int *node_stack_indexes, int stack_top,
	               int *should_rebalance)
	{
		node_t *gp, *p, *l, *s;
		int i = 0, pindex, sindex;
	
		*should_rebalance = 0;
	
		//> Root is a leaf, so nothing needs to be done
		if (node_stack[0]->leaf) return;
	
		gp = NULL;
		p  = node_stack[i++];
		pindex = node_stack_indexes[i-1];
		l  = node_stack[i++];
		while (!l->leaf && !l->tag && l->no_keys >= ABTREE_DEGREE_MIN) {
			gp = p;
			p = l;
			pindex = node_stack_indexes[i-1];
			l = node_stack[i++];
		}
	
		//> No violation to fix
		if (!l->tag && l->no_keys >= ABTREE_DEGREE_MIN) return;
	
		if (l->tag) {
			if (p->no_keys + l->no_keys <= ABTREE_DEGREE_MAX) {
				//> Join l with its parent
				join_parent_with_child(p, pindex, l);
			} else {
				//> Split child and parent
				split_parent_and_child(p, pindex, l);
				p->tag = (gp != NULL); //> Tag parent if not root
				*should_rebalance = 1;
			}
		} else if (l->no_keys < ABTREE_DEGREE_MIN) {
			sindex = pindex ? pindex - 1 : pindex + 1;
			s = (node_t*)p->children[sindex];
			if (s->tag) {
				//> FIXME
			} else {
				if (l->no_keys + s->no_keys + 1 <= ABTREE_DEGREE_MAX) {
					//> Join l and s
					join_siblings(p, l, s, pindex, sindex);
					*should_rebalance = (gp != NULL && p->no_keys < ABTREE_DEGREE_MIN);
				} else {
					//> Redistribute keys between s and l
					redistribute_sibling_keys(p, l, s, pindex, sindex);
				}
			}
		} else {
			assert(0);
		}
	}

	/**
	 * Splits a leaf node into two leaf nodes which also contain the newly
	 * inserted key 'key'.
	 **/ 
	node_t *leaf_split(node_t *n, int index, const K& key, const V& ptr)
	{
		int i, k=0, first_key_to_move = n->no_keys / 2;
		node_t *rnode = new node_t(1);
	
		//> Move half of the keys on the new node.
		for (i=first_key_to_move; i < n->no_keys; i++) {
			rnode->keys[k] = n->keys[i];
			rnode->children[k++] = n->children[i];
		}
		rnode->children[k] = n->children[i];
	
		//> Update number of keys for the two split nodes.
		n->no_keys -= k;
		rnode->no_keys = k;
	
		//> Insert the new key in the appropriate node.
		if (index < first_key_to_move) n->insert_index(index, key, (void*)ptr);
		else   rnode->insert_index(index - first_key_to_move, key, (void*)ptr);
	
		return rnode;
	}

	int do_insert(const K& key, const V& val,
	              node_t **node_stack, int *node_stack_indexes,
	              int *node_stack_top, int *should_rebalance)
	{
		node_t *n;
		int index, pindex;
	
		*should_rebalance = 0;
	
		//> Empty tree case.
		if (*node_stack_top == -1) {
			n = new node_t(1);
			n->insert_index(0, key, (void*)val);
			root = n;
			return 1;
		}
	
		n = node_stack[*node_stack_top];
		index = node_stack_indexes[*node_stack_top];
	
		//> Case of a not full leaf.
		if (n->no_keys < ABTREE_DEGREE_MAX) {
			n->insert_index(index, key, (void*)val);
			return 1;
		}
	
		//> Case of a not full leaf.
		node_t *rnode = leaf_split(n, index, key, val);
		node_t *parent_new = new node_t(0);
		parent_new->insert_index(0, rnode->keys[0], rnode);
		parent_new->children[0] = n;
		parent_new->tag = 1;
	
		//> We surpassed the root. New root needs to be created.
		if (*node_stack_top == 0) {
			root = parent_new;
			parent_new->tag = 0;
		} else {
			node_t *p = node_stack[*node_stack_top - 1];
			pindex = node_stack_indexes[*node_stack_top - 1];
			p->children[pindex] = parent_new;
		}
	
		//> Fix the node_stack for the rebalancing
		pindex = (key < parent_new->keys[0]) ? 0 : 1;
		node_stack[*node_stack_top] = parent_new;
		node_stack_indexes[*node_stack_top] = pindex;
		node_stack[++(*node_stack_top)] = (node_t*)parent_new->children[pindex];
		*should_rebalance = 1;
		return 1;
	}

	int insert_helper(const K& key, const V& val)
	{
		node_t *node_stack[MAX_HEIGHT];
		int node_stack_indexes[MAX_HEIGHT], node_stack_top = -1;
		int should_rebalance, ret;
	
		//> Route to the appropriate leaf.
		traverse_with_stack(key, node_stack, node_stack_indexes, &node_stack_top);
		int index = node_stack_indexes[node_stack_top];
		node_t *n = node_stack[node_stack_top];
		//> Key already in the tree.
		if (node_stack_top >= 0 && index < ABTREE_DEGREE_MAX && key == n->keys[index])
			return 0;
		//> Key not in the tree.
		ret = do_insert(key, val, node_stack, node_stack_indexes,
		                &node_stack_top, &should_rebalance);
		while (should_rebalance)
			rebalance(node_stack, node_stack_indexes, node_stack_top, &should_rebalance);
		return ret;
	}

	int do_delete(const K& key, node_t **node_stack,
	              int *node_stack_indexes, int node_stack_top,
	              int *should_rebalance)
	{
		node_t *n = node_stack[node_stack_top];
		int index = node_stack_indexes[node_stack_top];
		node_t *cur = node_stack[node_stack_top];
		int cur_index = node_stack_indexes[node_stack_top];
		node_t *parent;
		int parent_index;
	
		cur->delete_index(cur_index);
		*should_rebalance = cur->no_keys < ABTREE_DEGREE_MIN;
		return 1;
	}
	
	int delete_helper(const K& key)
	{
		int index;
		node_t *n;
		node_t *node_stack[MAX_HEIGHT];
		int node_stack_indexes[MAX_HEIGHT];
		int node_stack_top = -1;
		int should_rebalance, ret;
	
		//> Route to the appropriate leaf.
		traverse_with_stack(key, node_stack, node_stack_indexes, &node_stack_top);
	
		//> Empty tree case.
		if (node_stack_top == -1) return 0;
		n = node_stack[node_stack_top];
		index = node_stack_indexes[node_stack_top];
		//> Key not in the tree.
		if (index >= n->no_keys || key != n->keys[index])
			return 0;
		//> Key in the tree.
		ret = do_delete(key, node_stack, node_stack_indexes,
		                node_stack_top, &should_rebalance);
		while (should_rebalance)
			rebalance(node_stack, node_stack_indexes, node_stack_top, &should_rebalance);
		return ret;
	}

//static int abtree_update(abtree_t *abtree, key_t key, void *val)
//{
//	abtree_node_t *node_stack[MAX_HEIGHT];
//	int node_stack_indexes[MAX_HEIGHT], node_stack_top = -1;
//	int op_is_insert = -1, should_rebalance, ret;
//
//	//> Route to the appropriate leaf.
//	abtree_traverse_stack(abtree, key, node_stack, node_stack_indexes,
//	                     &node_stack_top);
//
//	//> Empty tree case.
//	if (node_stack_top == -1) {
//		op_is_insert = 1;
//	} else {
//		int index = node_stack_indexes[node_stack_top];
//		abtree_node_t *n = node_stack[node_stack_top];
//		if (index >= n->no_keys || key != n->keys[index]) op_is_insert = 1;
//		else if (index < ABTREE_DEGREE_MAX && key == n->keys[index]) op_is_insert = 0;
//	}
//	
//
//	if (op_is_insert)
//		ret = abtree_do_insert(abtree, key, val, node_stack, node_stack_indexes,
//		                       &node_stack_top, &should_rebalance);
//	else
//		ret = abtree_do_delete(abtree, key, node_stack, node_stack_indexes,
//		                       node_stack_top, &should_rebalance) + 2;
//	while (should_rebalance)
//		abtree_rebalance(abtree, node_stack, node_stack_indexes, node_stack_top,
//		                 &should_rebalance);
//	return ret;
//}

	void print_rec(node_t *root, int level)
	{
		int i;
	
		printf("[LVL %4d]: ", level);
		fflush(stdout);
		root->print();
	
		if (!root || root->leaf) return;
	
		for (i=0; i < root->no_keys; i++)
			print_rec((node_t *)root->children[i], level + 1);
		if (root->no_keys > 0)
			print_rec((node_t *)root->children[root->no_keys], level + 1);
	}
	
	void print_helper()
	{
		if (!root) {
			printf("Empty tree\n");
			return;
		}
	
		print_rec(root, 0);
	}

	int bst_violations, total_nodes, total_keys, leaf_keys;
	int null_children_violations;
	int not_full_nodes;
	int leaves_level;
	int leaves_at_same_level;

	/**
	 * Validates the following:
	 * 1. Keys inside node are sorted.
	 * 2. Keys inside node are higher than min and less than or equal to max.
	 * 3. Node is at least half-full.
	 * 4. Internal nodes do not have null children.
	 **/
	void node_validate(node_t *n, K min, K max)
	{
		K cur_min = n->keys[0];

		if (n != root && n->no_keys < ABTREE_DEGREE_MIN)
			not_full_nodes++;

		for (int i=1; i < n->no_keys; i++)
			if (n->keys[i] <= cur_min)
				bst_violations++;

		//> min != 0 is there to allow unsigned keys to be used and
		//> 0 value to represent KEY_MIN
		if ( (min != 0 && n->keys[0] < min) || n->keys[n->no_keys-1] > max)
			bst_violations++;

		if (!n->leaf)
			for (int i=0; i <= n->no_keys; i++)
				if (!n->children[i])
					null_children_violations++;
	}

	void validate_rec(node_t *root, K min, K max, int level)
	{
		if (!root) return;
	
		total_nodes++;
		total_keys += root->no_keys;
	
		node_validate(root, min, max);
		
		if (root->leaf) {
			if (leaves_level == -1) leaves_level = level;
			else if (level != leaves_level) leaves_at_same_level = 0;
			leaf_keys += root->no_keys;
			return;
		}
	
		for (int i=0; i <= root->no_keys; i++)
			validate_rec((node_t *)root->children[i],
			             i == 0 ? min : root->keys[i-1],
			             i == root->no_keys ? max : root->keys[i] - 1,
			             level+1);
	}
	
	int validate_helper()
	{
		int check_bst = 0, check_abtree_properties = 0;
		bst_violations = 0;
		total_nodes = 0;
		total_keys = leaf_keys = 0;
		null_children_violations = 0;
		not_full_nodes = 0;
		leaves_level = -1;
		leaves_at_same_level = 1;
	
		validate_rec(root, 0, ABTREE_MAX_KEY, 0);
	
		check_bst = (bst_violations == 0);
		check_abtree_properties = (null_children_violations == 0) &&
		                          (not_full_nodes == 0) &&
		                          (leaves_at_same_level == 1);
	
		printf("Validation:\n");
		printf("=======================\n");
		printf("  BST Violation: %s\n",
		       check_bst ? "No [OK]" : "Yes [ERROR]");
		printf("  ABTREE Violation: %s\n",
		       check_abtree_properties ? "No [OK]" : "Yes [ERROR]");
		printf("  |-- NULL Children Violation: %s\n",
		       (null_children_violations == 0) ? "No [OK]" : "Yes [ERROR]");
		printf("  |-- Not-full Nodes: %s\n",
		       (not_full_nodes == 0) ? "No [OK]" : "Yes [ERROR]");
		printf("  |-- Leaves at same level: %s [ Level %d ]\n",
		       (leaves_at_same_level == 1) ? "Yes [OK]" : "No [ERROR]", leaves_level);
		printf("  Tree size: %8d\n", total_nodes);
		printf("  Number of keys: %8d total / %8d in leaves\n", total_keys, leaf_keys);
		printf("\n");
	
		return check_bst && check_abtree_properties;
	}

public:

};

#define ABTREE_TEMPL template<typename K, typename V>
#define ABTREE_FUNCT abtree<K,V>

ABTREE_TEMPL
bool ABTREE_FUNCT::contains(const int tid, const K& key)
{
	return lookup_helper(key);
}

ABTREE_TEMPL
const std::pair<V,bool> ABTREE_FUNCT::find(const int tid, const K& key)
{
	int ret = lookup_helper(key);
	return std::pair<V,bool>(NULL, ret);
}

ABTREE_TEMPL
int ABTREE_FUNCT::rangeQuery(const int tid, const K& lo, const K& hi,
                             std::vector<std::pair<K,V>> kv_pairs)
{
	return 0;
}

ABTREE_TEMPL
const V ABTREE_FUNCT::insert(const int tid, const K& key, const V& val)
{
	return insertIfAbsent(tid, key, val);
}

ABTREE_TEMPL
const V ABTREE_FUNCT::insertIfAbsent(const int tid, const K& key, const V& val)
{
	int ret = insert_helper(key, val);
	if (ret == 1) return NULL;
	else return (void *)1;
}

ABTREE_TEMPL
const std::pair<V,bool> ABTREE_FUNCT::remove(const int tid, const K& key)
{
	int ret = delete_helper(key);
	if (ret == 0) return std::pair<V,bool>(NULL, false);
	else return std::pair<V,bool>((void*)1, true);
}

ABTREE_TEMPL
bool ABTREE_FUNCT::validate()
{
	return validate_helper();
}
