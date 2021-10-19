/**
 * A B+-tree.
 **/

/**
 * Note about the used values:
 *   - Need to be of pointer type, because we have node_t::children to be void *
 **/

#pragma once

#include "../rcu-htm/ht.h"
#include "../map_if.h"
#include "Log.h"

#define BTREE_MAX_KEY 99999999999LLU

template <typename K, typename V>
class btree : public Map<K,V> {
private:
	static const unsigned int NODE_ORDER = 8;

public:
	btree(const K _NO_KEY, const V _NO_VALUE, const int numProcesses)
	  : Map<K,V>(_NO_KEY, _NO_VALUE)
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
	char *name() { return "B+-tree"; }

	void print() { print_helper(); };
//	unsigned long long size() { return size_rec(root); };

private:

	struct node_t {
		bool leaf;
	
		int no_keys;
		K keys[2*NODE_ORDER];
		__attribute__((aligned(16))) void *children[2*NODE_ORDER + 1];

		node_t (bool leaf) {
			this->no_keys = 0;
			this->leaf = leaf;
		}

		node_t *copy() {
			node_t *newn = new node_t(this->leaf);
			newn->no_keys = this->no_keys;
			for (int i=0; i < 2*NODE_ORDER; i++)
				newn->keys[i] = this->keys[i];
			for (int i=0; i < 2*NODE_ORDER + 1; i++)
				newn->children[i] = this->children[i];
			return newn;
		}

		int search(const K& key)
		{
			int i = 0;
			while (i < no_keys && key > keys[i]) i++;
			return i;
		}
		
		/**
		 * Distributes the keys of 'this' on the two nodes and also adds 'key'.
		 * The distribution is done so as 'this' contains NODE_ORDER + 1 keys
		 * and 'rnode' contains NODE_ORDER keys.
		 * CAUTION: rnode->children[0] is left NULL.
		 **/
		void distribute_keys(node_t *rnode, const K& key,
		                     void *ptr, int index)
		{
			int i, mid;
		
			mid = NODE_ORDER;
			if (index > NODE_ORDER) mid++;
		
			//> Move half of the keys on the new node.
			for (i = mid; i < 2 * NODE_ORDER; i++) {
				rnode->keys[i - mid] = this->keys[i];
				rnode->children[i - mid] = this->children[i];
			}
			rnode->children[i - mid] = this->children[i];
		
			this->no_keys = mid;
			rnode->no_keys = 2 * NODE_ORDER - mid;
		
			//> Insert the new key in the appropriate node.
			if (index > NODE_ORDER) rnode->insert_index(index - mid, key, ptr);
			else this->insert_index(index, key, ptr);
		}

		node_t *split(const K& key, void *ptr, int index, K *key_ret)
		{
//			node_t *rnode = btree_node_new(n->leaf);
			node_t *rnode = new node_t(this->leaf);
			this->distribute_keys(rnode, key, ptr, index);
		
			//> This is the key that will be propagated upwards.
			*key_ret = this->keys[NODE_ORDER];
		
			if (!this->leaf) {
				rnode->children[0] = this->children[this->no_keys];
				this->no_keys--;
			}
		
			return rnode;
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

		void print()
		{
			int i;
		
			printf("btree_node: [");
		
			for (i=0; i < no_keys; i++)
				printf(" %llu |", keys[i]);
			printf("]");
			printf("%s\n", leaf ? " LEAF" : "");
		}

	};

	node_t *root;

private:

	const V lookup_helper(const K& key)
	{
		int index;
		node_t *n = root;
	
		//> Empty tree.
		if (!n) return this->NO_VALUE;
	
		while (!n->leaf) {
			index = n->search(key);
			n = (node_t *)n->children[index];
		}
		index = n->search(key);
		return (V)n->children[index+1];
	}

	const V insert_helper(const K& key, const V& val)
	{
		int index;
		node_t *n, *rnode;
		node_t *node_stack[20];
		int node_stack_indexes[20];
		int stack_top = -1;
	
		//> Route to the appropriate leaf.
		traverse_with_stack(key, node_stack, node_stack_indexes, &stack_top);
		if (stack_top >= 0 && node_stack_indexes[stack_top] < 2 * NODE_ORDER &&
		        node_stack[stack_top]->keys[node_stack_indexes[stack_top]] == key)
			return (V)node_stack[stack_top]->children[node_stack_indexes[stack_top] + 1];
	
		n = NULL;
		K key_to_add = key;
		void *ptr_to_add = val;
	
		while (1) {
			//> We surpassed the root. New root needs to be created.
			if (stack_top < 0) {
				root = new node_t(n == NULL ? 1 : 0);
				root->insert_index(0, key_to_add, ptr_to_add);
				root->children[0] = n;
				break;
			}
	
			n = node_stack[stack_top];
			index = node_stack_indexes[stack_top];
	
			//> No split required.
			if (n->no_keys < 2 * NODE_ORDER) {
				n->insert_index(index, key_to_add, ptr_to_add);
				break;
			}
	
			ptr_to_add = n->split(key_to_add, ptr_to_add, index, &key_to_add);
	
			stack_top--;
		}
	
		return this->NO_VALUE;
	}

	/**
	 * c = current
	 * p = parent
	 * pindex = parent_index
	 * Returns: the index of the key to be deleted from the parent node.
	 **/
	int merge(node_t *c, node_t *p, int pindex)
	{
		int i, sibling_index;
		node_t *sibling;
	
		//> Left sibling first.
		if (pindex > 0) {
			sibling = (node_t *)p->children[pindex - 1];
			sibling_index = sibling->no_keys;
	
			if (!c->leaf) {
				sibling->keys[sibling_index] = p->keys[pindex - 1];
				sibling->children[sibling_index+1] = c->children[0];
				sibling_index++;
			}
			for (i=0; i < c->no_keys; i++) {
				sibling->keys[sibling_index] = c->keys[i];
				sibling->children[sibling_index + 1] = c->children[i + 1];
				sibling_index++;
			}
	
			sibling->no_keys = sibling_index;
			return (pindex - 1);
		}
	
		//> Right sibling next
		if (pindex < p->no_keys) {
			sibling = (node_t *)p->children[pindex + 1];
			sibling_index = c->no_keys;
	
			if (!c->leaf) {
				c->keys[sibling_index] = p->keys[pindex];
				c->children[sibling_index+1] = sibling->children[0];
				sibling_index++;
			}
			for (i=0; i < sibling->no_keys; i++) {
				c->keys[sibling_index] = sibling->keys[i];
				c->children[sibling_index + 1] = sibling->children[i + 1];
				sibling_index++;
			}
	
			c->no_keys = sibling_index;
			return pindex;
		}
	
		//> Unreached code.
		assert(0);
		return -1;
	}
	
	/**
	 * c = current
	 * p = parent
	 * pindex = parent_index
	 * Returns: 1 if borrowing was successful, 0 otherwise.
	 **/
	int borrow_keys(node_t *c, node_t *p, int pindex)
	{
		int i;
		node_t *sibling;
	
		//> Left sibling first.
		if (pindex > 0) {
			sibling = (node_t *)p->children[pindex - 1];
			if (sibling->no_keys > NODE_ORDER) {
				for (i = c->no_keys-1; i >= 0; i--) c->keys[i+1] = c->keys[i];
				for (i = c->no_keys; i >= 0; i--) c->children[i+1] = c->children[i];
				if (!c->leaf) {
					if (c->keys[0] == p->keys[pindex-1])
						c->keys[0] = sibling->keys[sibling->no_keys-1];
					else
						c->keys[0] = p->keys[pindex-1];
					c->children[0] = sibling->children[sibling->no_keys];
					p->keys[pindex-1] = sibling->keys[sibling->no_keys-1];
				} else {
					c->keys[0] = sibling->keys[sibling->no_keys-1];
					c->children[1] = sibling->children[sibling->no_keys];
					p->keys[pindex-1] = sibling->keys[sibling->no_keys-2];
				}
				sibling->no_keys--;
				c->no_keys++;
				return 1;
			}
		}
	
		//> Right sibling next.
		if (pindex < p->no_keys) {
			sibling = (node_t *)p->children[pindex + 1];
			if (sibling->no_keys > NODE_ORDER) {
				if (!c->leaf) {
					c->keys[c->no_keys] = p->keys[pindex];
					c->children[c->no_keys+1] = sibling->children[0];
					p->keys[pindex] = sibling->keys[0];
				} else {
					c->keys[c->no_keys] = sibling->keys[0];
					c->children[c->no_keys+1] = sibling->children[1];
					p->keys[pindex] = c->keys[c->no_keys];
				}
				for (i=0; i < sibling->no_keys-1; i++)
					sibling->keys[i] = sibling->keys[i+1];
				for (i=0; i < sibling->no_keys; i++)
					sibling->children[i] = sibling->children[i+1];
				sibling->no_keys--;
				c->no_keys++;
				return 1;
			}
		}
	
		//> Could not borrow for either of the two siblings.
		return 0;
	}
	
	void do_delete(const K& key, node_t **node_stack, int *node_stack_indexes,
	               int node_stack_top)
	{
		node_t *n = node_stack[node_stack_top];
		int index = node_stack_indexes[node_stack_top];
		node_t *cur = node_stack[node_stack_top];
		int cur_index = node_stack_indexes[node_stack_top];
		node_t *parent;
		int parent_index;
		while (1) {
			//> We reached root which contains only one key.
			if (node_stack_top == 0 && cur->no_keys == 1) {
				root = (node_t *)cur->children[0];
				break;
			}
	
			//> Delete the key from the current node.
			cur->delete_index(cur_index);
	
			//> Root can be less than half-full.
			if (node_stack_top == 0) break;
	
			//> If current node is at least half-full, we are done.
			if (cur->no_keys >= NODE_ORDER)
				break;
	
			//> First try to borrow keys from siblings
			parent = node_stack[node_stack_top-1];
			parent_index = node_stack_indexes[node_stack_top-1];
			if (borrow_keys(cur, parent, parent_index))
				break;
	
			//> If everything has failed, merge nodes
			cur_index = merge(cur, parent, parent_index);
	
			//> Move one level up
			cur = node_stack[--node_stack_top];
		}
	}
	
	const V delete_helper(const K& key)
	{
		int index;
		node_t *n;
		node_t *node_stack[20];
		int node_stack_indexes[20];
		int node_stack_top = -1;
	
		//> Route to the appropriate leaf.
		traverse_with_stack(key, node_stack, node_stack_indexes, &node_stack_top);
	
		//> Empty tree case.
		if (node_stack_top == -1) return this->NO_VALUE;
		//> Key not in the tree.
		n = node_stack[node_stack_top];
		index = node_stack_indexes[node_stack_top];
		if (index >= n->no_keys || key != n->keys[index]) return this->NO_VALUE;
	
		const V del_val = (V)n->children[index+1];
		do_delete(key, node_stack, node_stack_indexes, node_stack_top);
		return del_val;
	}
	
//	int btree_update(const K& key, const V& val)
//	{
//		node_t *node_stack[20];
//		int node_stack_indexes[20], node_stack_top = -1;
//		int op_is_insert = -1;
//	
//		//> Route to the appropriate leaf.
//		traverse_with_stack(key, node_stack, node_stack_indexes, &node_stack_top);
//	
//		//> Empty tree case.
//		if (node_stack_top == -1) {
//			op_is_insert = 1;
//		} else {
//			int index = node_stack_indexes[node_stack_top];
//			node_t *n = node_stack[node_stack_top];
//			if (index >= n->no_keys || key != n->keys[index]) op_is_insert = 1;
//			else if (index < 2 * NODE_ORDER && key == n->keys[index]) op_is_insert = 0;
//		}
//		
//	
//		if (op_is_insert)
//			return do_insert(key, val, node_stack, node_stack_indexes, node_stack_top);
//		else
//			return do_delete(key, node_stack, node_stack_indexes, node_stack_top) + 2;
//	}

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
		if (!root) printf("Empty tree\n");
		else       print_rec(root, 0);
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
		int i;
		K cur_min = n->keys[0];

		if (n != root && n->no_keys < NODE_ORDER)
			not_full_nodes++;

		for (i=1; i < n->no_keys; i++)
			if (n->keys[i] <= cur_min) {
				bst_violations++;
			}

		if ( (min != MIN_KEY && n->keys[0] <= min) || n->keys[n->no_keys-1] > max) {
			bst_violations++;
		}

		if (!n->leaf)
			for (i=0; i <= n->no_keys; i++)
				if (!n->children[i])
					null_children_violations++;
	}

	void validate_rec(node_t *root, K min, K max, int level)
	{
		int i;
	
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
	
		for (i=0; i <= root->no_keys; i++)
			validate_rec((node_t *)root->children[i],
			             i == 0 ? min : root->keys[i-1],
			             i == root->no_keys ? max : root->keys[i],
			             level+1);
	}
	
	int validate_helper()
	{
		int check_bst = 0, check_btree_properties = 0;
		bst_violations = 0;
		total_nodes = 0;
		total_keys = leaf_keys = 0;
		null_children_violations = 0;
		not_full_nodes = 0;
		leaves_level = -1;
		leaves_at_same_level = 1;
	
		validate_rec(root, MIN_KEY, BTREE_MAX_KEY, 0);
	
		check_bst = (bst_violations == 0);
		check_btree_properties = (null_children_violations == 0) &&
		                         (not_full_nodes == 0) &&
		                         (leaves_at_same_level == 1);
	
		printf("Validation:\n");
		printf("=======================\n");
		printf("  BST Violation: %s\n",
		       check_bst ? "No [OK]" : "Yes [ERROR]");
		printf("  BTREE Violation: %s\n",
		       check_btree_properties ? "No [OK]" : "Yes [ERROR]");
		printf("  |-- NULL Children Violation: %s\n",
		       (null_children_violations == 0) ? "No [OK]" : "Yes [ERROR]");
		printf("  |-- Not-full Nodes: %s\n",
		       (not_full_nodes == 0) ? "No [OK]" : "Yes [ERROR]");
		printf("  |-- Leaves at same level: %s [ Level %d ]\n",
		       (leaves_at_same_level == 1) ? "Yes [OK]" : "No [ERROR]", leaves_level);
		printf("  Tree size: %8d\n", total_nodes);
		printf("  Number of keys: %8d total / %8d in leaves\n", total_keys, leaf_keys);
		printf("\n");
	
		return check_bst && check_btree_properties;
	}

public:

	void validate_copy(void **node_stack_, int *node_stack_indexes,
	                   int stack_top)
	{
		node_t **node_stack = (node_t **)node_stack_;

		//> Validate copy
		if (stack_top < 0 && root != NULL)
			TX_ABORT(ABORT_VALIDATION_FAILURE);
		if (stack_top >= 0 && root != node_stack[0])
			TX_ABORT(ABORT_VALIDATION_FAILURE);
		node_t *n1, *n2;
		for (int i=0; i < stack_top; i++) {
			n1 = node_stack[i];
			int index = node_stack_indexes[i];
			n2 = (node_t *)n1->children[index];
			if (n2 != node_stack[i+1])
				TX_ABORT(ABORT_VALIDATION_FAILURE);
		}
		for (int i=0; i < HT_LEN; i++) {
			for (int j=0; j < tdata->ht->bucket_next_index[i]; j+=2) {
				node_t **np = (node_t **)tdata->ht->entries[i][j];
				node_t  *n  = (node_t *)tdata->ht->entries[i][j+1];
				if (*np != n) TX_ABORT(ABORT_VALIDATION_FAILURE);
			}
		}
	}

	const V traverse_with_stack(const K& key, node_t **node_stack,
	                            int *node_stack_indexes, int *node_stack_top)
	{
		return traverse_with_stack(key, (void **)node_stack,
		                           node_stack_indexes, node_stack_top);
	}

	const V traverse_with_stack(const K& key, void **node_stack_,
	                            int *node_stack_indexes, int *node_stack_top)
	{
		node_t **node_stack = (node_t **)node_stack_;
		int index;
		node_t *n;
	
		*node_stack_top = -1;
		n = root;
		if (!n) return this->NO_VALUE;
	
		while (!n->leaf) {
			index = n->search(key);
			node_stack[++(*node_stack_top)] = n;
			node_stack_indexes[*node_stack_top] = index;
			n = (node_t *)n->children[index];
		}
		index = n->search(key);
		node_stack[++(*node_stack_top)] = n;
		node_stack_indexes[*node_stack_top] = index;

		index = node_stack_indexes[*node_stack_top];
		if (*node_stack_top >= 0 && index < n->no_keys
		                         && n->keys[index] == key)
			return (V)n->children[index+1];
		return this->NO_VALUE;
	}

	/**
	 * Inserts 'key' in the tree and returns the connection point.
	 **/
	void *insert_with_copy(const K& key, const V& val,
	                       void **node_stack_, int *node_stack_indexes,
	                       int stack_top,
	                       void **tree_cp_root_,
	                       int *connection_point_stack_index)
	{
		node_t **node_stack = (node_t **)node_stack_;
		node_t **tree_cp_root = (node_t **)tree_cp_root_;

		node_t *cur = NULL, *cur_cp = NULL, *cur_cp_prev;
		node_t *conn_point;
		int index, i;
		K key_to_add = key;
		void *ptr_to_add = val;
	
		while (1) {
			//> We surpassed the root. New root needs to be created.
			if (stack_top < 0) {
				node_t *new_node = new node_t(cur == NULL ? true : false);
				new_node->insert_index(0, key_to_add, ptr_to_add);
				new_node->children[0] = cur_cp;
				*tree_cp_root = new_node;
				break;
			}
	
			cur = node_stack[stack_top];
			index = node_stack_indexes[stack_top];
	
			//> Copy current node
			cur_cp_prev = cur_cp;
			cur_cp = cur->copy();
			for (i=0; i <= cur_cp->no_keys; i++)
				ht_insert(tdata->ht, &cur->children[i], cur_cp->children[i]);
	
			//> Connect copied node with the rest of the copied tree.
			if (cur_cp_prev) cur_cp->children[index] = cur_cp_prev;
	
			//> No split required.
			if (cur_cp->no_keys < 2 * NODE_ORDER) {
				cur_cp->insert_index(index, key_to_add, ptr_to_add);
				*tree_cp_root = cur_cp;
				break;
			}
	
			ptr_to_add = cur_cp->split(key_to_add, ptr_to_add, index, &key_to_add);
	
			stack_top--;
		}
	
		*connection_point_stack_index = stack_top - 1;
		conn_point = stack_top <= 0 ? NULL : node_stack[stack_top-1];
		return (void *)conn_point;
	}

	void install_copy(void *connpoint_, void *privcopy_,
	                  int *node_stack_indexes, int connection_point_stack_index)
	{
		node_t *connpoint = (node_t *)connpoint_;
		node_t *privcopy = (node_t *)privcopy_;

		if (connpoint == NULL) {
			root = privcopy;
		} else {
			int index = node_stack_indexes[connection_point_stack_index];
			connpoint->children[index] = privcopy;
		}
	}


	/**
	 * c = current
	 * p = parent
	 * pindex = parent_index
	 * Returns: pointer to the node that remains.
	 **/
	node_t *merge_with_copy(node_t *c, node_t *p, int pindex,
	                        int *merged_with_left_sibling,
	                        node_t *sibling_left,
	                        node_t *sibling_right)
	{
		int i, sibling_index;
		node_t *sibling, *sibling_cp;
	
		//> Left sibling first.
		if (pindex > 0) {
			sibling = sibling_left;
			sibling_cp = sibling->copy();
			for (i=0; i <= sibling_cp->no_keys; i++)
				ht_insert(tdata->ht, &sibling->children[i], sibling_cp->children[i]);
	
			sibling_index = sibling_cp->no_keys;
	
			if (!c->leaf) {
				sibling_cp->keys[sibling_index] = p->keys[pindex - 1];
				sibling_cp->children[sibling_index+1] = c->children[0];
				sibling_index++;
			}
			for (i=0; i < c->no_keys; i++) {
				sibling_cp->keys[sibling_index] = c->keys[i];
				sibling_cp->children[sibling_index + 1] = c->children[i + 1];
				sibling_index++;
			}
	
			sibling_cp->no_keys = sibling_index;
			*merged_with_left_sibling = 1;
			return sibling_cp;
		}
	
		//> Right sibling next
		if (pindex < p->no_keys) {
			sibling = sibling_right;
			sibling_cp = sibling->copy();
			ht_insert(tdata->ht, &p->children[pindex+1], sibling);
			for (i=0; i <= sibling_cp->no_keys; i++)
				ht_insert(tdata->ht, &sibling->children[i], sibling_cp->children[i]);
	
			sibling_index = c->no_keys;
	
			if (!c->leaf) {
				c->keys[sibling_index] = p->keys[pindex];
				c->children[sibling_index+1] = sibling_cp->children[0];
				sibling_index++;
			}
			for (i=0; i < sibling_cp->no_keys; i++) {
				c->keys[sibling_index] = sibling_cp->keys[i];
				c->children[sibling_index + 1] = sibling_cp->children[i + 1];
				sibling_index++;
			}
	
			c->no_keys = sibling_index;
			*merged_with_left_sibling = 0;
			return c;
		}
	
		//> Unreached code.
		assert(0);
		return NULL;
	}

	/**
	 * c = current
	 * p = parent
	 * pindex = parent_index
	 * Returns: parent_cp if borrowing was successful, NULL otherwise.
	 **/
	node_t *borrow_keys_with_copies(node_t *c, node_t *p, int pindex,
	                                node_t **sibling_left,
	                                node_t **sibling_right)
	{
		int i;
		node_t *sibling, *sibling_cp, *parent_cp;
	
		//> Left sibling first.
		if (pindex > 0) {
			sibling = (node_t *)p->children[pindex - 1];
			*sibling_left = sibling;
			ht_insert(tdata->ht, &p->children[pindex-1], sibling);
			if (sibling->no_keys > NODE_ORDER) {
				sibling_cp = sibling->copy();
				parent_cp = p->copy();
				for (i=0; i <= sibling_cp->no_keys; i++)
					ht_insert(tdata->ht, &sibling->children[i], sibling_cp->children[i]);
				for (i=0; i <= parent_cp->no_keys; i++)
					ht_insert(tdata->ht, &p->children[i], parent_cp->children[i]);
	
				parent_cp->children[pindex - 1] = sibling_cp;
				parent_cp->children[pindex] = c;
	
				for (i = c->no_keys-1; i >= 0; i--) c->keys[i+1] = c->keys[i];
				for (i = c->no_keys; i >= 0; i--) c->children[i+1] = c->children[i];
				if (!c->leaf) {
					c->keys[0] = parent_cp->keys[pindex-1];
					c->children[0] = sibling_cp->children[sibling_cp->no_keys];
					parent_cp->keys[pindex-1] = sibling_cp->keys[sibling_cp->no_keys-1];
				} else {
					c->keys[0] = sibling_cp->keys[sibling_cp->no_keys-1];
					c->children[1] = sibling_cp->children[sibling_cp->no_keys];
					parent_cp->keys[pindex-1] = sibling_cp->keys[sibling_cp->no_keys-2];
				}
				sibling_cp->no_keys--;
				c->no_keys++;
				return parent_cp;
			}
		}
	
		//> Right sibling next.
		if (pindex < p->no_keys) {
			sibling = (node_t *)p->children[pindex + 1];
			*sibling_right = sibling;
			ht_insert(tdata->ht, &p->children[pindex+1], sibling);
			if (sibling->no_keys > NODE_ORDER) {
				sibling_cp = sibling->copy();
				parent_cp = p->copy();
				for (i=0; i <= sibling_cp->no_keys; i++)
					ht_insert(tdata->ht, &sibling->children[i], sibling_cp->children[i]);
				for (i=0; i <= parent_cp->no_keys; i++)
					ht_insert(tdata->ht, &p->children[i], parent_cp->children[i]);
	
				parent_cp->children[pindex] = c;
				parent_cp->children[pindex+1] = sibling_cp;
	
				if (!c->leaf) {
					c->keys[c->no_keys] = parent_cp->keys[pindex];
					c->children[c->no_keys+1] = sibling_cp->children[0];
					parent_cp->keys[pindex] = sibling_cp->keys[0];
				} else {
					c->keys[c->no_keys] = sibling_cp->keys[0];
					c->children[c->no_keys+1] = sibling_cp->children[1];
					parent_cp->keys[pindex] = c->keys[c->no_keys];
				}
				for (i=0; i < sibling_cp->no_keys-1; i++)
					sibling_cp->keys[i] = sibling_cp->keys[i+1];
				for (i=0; i < sibling_cp->no_keys; i++)
					sibling_cp->children[i] = sibling_cp->children[i+1];
				sibling_cp->no_keys--;
				c->no_keys++;
				return parent_cp;
			}
		}
	
		//> Could not borrow for either of the two siblings.
		return NULL;
	}

	void *delete_with_copy(const K& key,
	                       void **node_stack_, int *node_stack_indexes,
	                       int *_stack_top,
	                       void **tree_cp_root_,
	                       int *connection_point_stack_index)
	{
		node_t **node_stack = (node_t **)node_stack_;
		node_t **tree_cp_root = (node_t **)tree_cp_root_;
		node_t *parent;
		int parent_index;
		node_t *cur = NULL, *cur_cp = NULL, *cur_cp_prev;
		node_t *conn_point, *new_parent;
		int index, i;
		int merged_with_left_sibling = 0;
		int stack_top = *_stack_top;
	
		*tree_cp_root = NULL;
	
		while (1) {
			cur = node_stack[stack_top];
	
			//> We reached root which contains only one key.
			if (stack_top == 0 && cur->no_keys == 1) break;
	
			index = node_stack_indexes[stack_top];
			if (merged_with_left_sibling) index--;
	
			//> Copy current node
			cur_cp = cur->copy();
			for (i=0; i <= cur_cp->no_keys; i++)
				ht_insert(tdata->ht, &cur->children[i], cur_cp->children[i]);
	
			//> Connect copied node with the rest of the copied tree.
			if (*tree_cp_root) cur_cp->children[index] = *tree_cp_root;
			*tree_cp_root = cur_cp;
	
			//> Delete the key from the current node.
			cur_cp->delete_index(index);
	
			//> Root can be less than half-full.
			if (stack_top == 0) break;
	
			//> If current node is at least half-full, we are done.
			if (cur_cp->no_keys >= NODE_ORDER) break;
	
			//> First try to borrow keys from siblings
			node_t *sibling_left = NULL, *sibling_right = NULL;
			parent = node_stack[stack_top-1];
			parent_index = node_stack_indexes[stack_top-1];
			new_parent = borrow_keys_with_copies(cur_cp, parent, parent_index,
			                                     &sibling_left, &sibling_right);
			if (new_parent != NULL) {
				*tree_cp_root = new_parent;
				stack_top--;
				break;
			}
	
			//> If everything has failed, merge nodes
			*tree_cp_root = merge_with_copy(cur_cp, parent, parent_index,
			                                &merged_with_left_sibling,
			                                sibling_left, sibling_right);
	
			//> Move one level up
			stack_top--;
		}
	
		*connection_point_stack_index = stack_top - 1;
		conn_point = stack_top <= 0 ? NULL : node_stack[stack_top-1];
		return conn_point;
	}

};

#define BTREE_TEMPL template<typename K, typename V>
#define BTREE_FUNCT btree<K,V>

BTREE_TEMPL
bool BTREE_FUNCT::contains(const int tid, const K& key)
{
	const V ret = lookup_helper(key);
	return (ret != this->NO_VALUE);
}

BTREE_TEMPL
const std::pair<V,bool> BTREE_FUNCT::find(const int tid, const K& key)
{
	const V ret = lookup_helper(key);
	return std::pair<V, bool>(ret, ret != this->NO_VALUE);
}

BTREE_TEMPL
int BTREE_FUNCT::rangeQuery(const int tid, const K& lo, const K& hi,
                      std::vector<std::pair<K,V>> kv_pairs)
{
	return 0;
}

BTREE_TEMPL
const V BTREE_FUNCT::insert(const int tid, const K& key, const V& val)
{
	return insertIfAbsent(tid, key, val);
}

BTREE_TEMPL
const V BTREE_FUNCT::insertIfAbsent(const int tid, const K& key, const V& val)
{
	return insert_helper(key, val);
}

BTREE_TEMPL
const std::pair<V,bool> BTREE_FUNCT::remove(const int tid, const K& key)
{
	const V ret = delete_helper(key);
	return std::pair<V,bool>(ret, ret != this->NO_VALUE);
}

BTREE_TEMPL
bool BTREE_FUNCT::validate()
{
	return validate_helper();
}
