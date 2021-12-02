/**
 * A partially external binary search tree.
 **/

#pragma once

#include "../rcu-htm/ht.h"
#include "../map_if.h"
#include "Log.h"

template <typename K, typename V>
class bst_unb_pext : public Map<K,V> {
public:
	bst_unb_pext(const K _NO_KEY, const V _NO_VALUE, const int numProcesses)
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
	char *name() { return "BST Unbalanced Partially-External"; }

	void print() { print_helper(); };
	unsigned long long size() { return size_rec(root); };

private:

	typedef bst_unb_pext<K, V> tree_t;

	struct node_t {
		K key;
		V value;
		node_t *left, *right;
		bool marked;

		node_t (const K& key_, const V& value_) :
		     key(key_), value(value_),
		     left(NULL), right(NULL), marked(false) {};
	};

	inline node_t *node_new_copy(node_t *src)
	{
		node_t *ret = new node_t(src->key, src->value);
		ret->left = src->left;
		ret->right = src->right;
		ret->marked = src->marked;
		return ret;
	}

	node_t *root;

private:
	/**
	 * Used by CA but not from the public interface.
	 **/
	//> Removes the max key node from the tree and returns it.
	node_t *get_max_key_node(tree_t *tree)
	{
		assert(tree->root != NULL);

		if (tree->root->right == NULL) {
			node_t *ret = tree->root;
			tree->root = tree->root->left;
			return ret;
		}

		node_t *prev = tree->root, *curr = tree->root->right;
		while (curr->right != NULL) {
			prev = curr;
			curr = curr->right;
		}

		node_t *ret = curr;
		prev->right = curr->left;
		return ret;
	}
	void add_max_key_node(tree_t *tree, node_t *n)
	{
		n->left = n->right = NULL;

		if (tree->root == NULL) {
			tree->root = n;
			return;
		}

		node_t *curr = tree->root;
		while (curr->right != NULL) curr = curr->right;
		curr->right = n;
	}

public:
	/**
	 * CA-locks adapting methods.
	 **/
	const K& max_key()
	{
		assert(root != NULL);
		node_t *curr = root;
		while (curr->right != NULL) curr = curr->right;
		return curr->key;
	}

	const K& min_key()
	{
		assert(root != NULL);
		node_t *curr = root;
		while (curr->left != NULL) curr = curr->left;
		return curr->key;
	}

	//> Splits the tree in two trees.
	//> The left part is returned and the right part is put in *right_part
	void *split(void **_right_part)
	{
		tree_t **right_part = (tree_t **)_right_part;
		tree_t *right_tree;

		*right_part = NULL;
		if (!root) return NULL;

		right_tree = new tree_t(this->INF_KEY, this->NO_VALUE, 88);
		right_tree->root = root->right;
		*right_part = right_tree;

		node_t *old_root = root;
		root = root->left;
		add_max_key_node(this, old_root);
		return (void *)this;
	}

	//> Joins 'this' and tree_right and returns the joint tree
	void *join(void *_tree_right)
	{
		tree_t *tree_right = (tree_t *)_tree_right;
		tree_t *tree_left = this;
		node_t *new_internal;

		if (!tree_left->root) return tree_right;
		else if (!tree_right->root) return tree_left;

		node_t *max_key_node = get_max_key_node(tree_left);
		max_key_node->left = tree_left->root;
		max_key_node->right = tree_right->root;
		tree_left->root = max_key_node;
		return (void *)tree_left;
	}

	bool is_empty() { return (root == NULL); }
	long long get_key_sum() { return get_key_sum_rec(root); }

	long long get_key_sum_rec(node_t *n)
	{
		if (!n) return 0;
		long long n_sum = (long long)(n->marked ? 0 : n->key);
		return get_key_sum_rec(n->left) + n_sum + get_key_sum_rec(n->right);
	}

	bool validate(bool print) { return validate_helper(print); };

	unsigned long long size_rec(node_t *n)
	{
		if (n == NULL) return 0;
		return size_rec(n->left) + (n->marked ? 0 : 1) + size_rec(n->right);
	}

public:
	/**
	 * RCU-HTM adapting methods.
	 **/
	const V traverse_with_stack(const K& key, void **stack,
	                            int *stack_indexes, int *stack_top)
	{
		node_t *parent, *leaf;
		node_t **node_stack = (node_t **)stack;
	
		parent = NULL;
		leaf = root;
		*stack_top = -1;
	
		while (leaf) {
			node_stack[++(*stack_top)] = leaf;
			stack_indexes[*stack_top] = (key <= leaf->key) ? 0 : 1;

			if (leaf->key == key) break;
			parent = leaf;
			leaf = (key <= leaf->key) ? leaf->left : leaf->right;
		}

		if (*stack_top >= 0 && !node_stack[*stack_top]->marked
		                    && node_stack[*stack_top]->key == key)
			return node_stack[*stack_top]->value;
		else
			return this->NO_VALUE;
	}
	void install_copy(void *connpoint, void *privcopy,
	                  int *node_stack_indexes, int connpoint_stack_index)
	{
		node_t *connection_point = (node_t *)connpoint;
		node_t *tree_copy_root = (node_t *)privcopy;
		if (!connection_point) {
			root = tree_copy_root;
		} else {
			int index = node_stack_indexes[connpoint_stack_index];
			if (index == 0) connection_point->left = tree_copy_root;
			else            connection_point->right = tree_copy_root;
		}

	}
	void validate_copy(void **stack, int *node_stack_indexes,
	                   int stack_top)
	{
		node_t **node_stack = (node_t **)stack;
		node_t *n1, *n2;

		if (stack_top < 0 && root != NULL)
			TX_ABORT(ABORT_VALIDATION_FAILURE);
		if (stack_top >= 0 && root != node_stack[0])
			TX_ABORT(ABORT_VALIDATION_FAILURE);

		for (int i=0; i < stack_top; i++) {
			n1 = node_stack[i];
			int index = node_stack_indexes[i];
			n2 = (node_t *)((index == 0) ? n1->left : n1->right);
			if (n2 != node_stack[i+1])
					TX_ABORT(ABORT_VALIDATION_FAILURE);
		}
	
		for (int i=0; i < HT_LEN; i++) {
			for (int j=0; j < tdata->ht->bucket_next_index[i]; j+=2) {
				node_t **np = (node_t **)tdata->ht->entries[i][j];
				node_t  *n  = (node_t *)tdata->ht->entries[i][j+1];
				if (*np != n)
					TX_ABORT(ABORT_VALIDATION_FAILURE);
			}
		}

	}
	void *insert_with_copy(const K& key, const V& value, void **stack,
	                       int *stack_indexes, int stack_top, void **privcopy,
	                       int *connpoint_stack_index)
	{
		node_t **node_stack = (node_t **)stack;
		node_t *new_node;
		node_t *connection_point;

		if (stack_top >= 0 && key < node_stack[stack_top]->key)
			ht_insert(tdata->ht, &node_stack[stack_top]->left, NULL);
		else if (stack_top >= 0 && key > node_stack[stack_top]->key)
			ht_insert(tdata->ht, &node_stack[stack_top]->right, NULL);

		//> If tree is empty
		if (stack_top == -1) {
			*privcopy = new node_t(key, value);
			*connpoint_stack_index = -1;
			return NULL;
		}

		node_t *n = node_stack[stack_top];
		if (n->key == key && n->marked) {
			new_node = node_new_copy(n);
			ht_insert(tdata->ht, &n->left, new_node->left);
			ht_insert(tdata->ht, &n->right, new_node->right);
			new_node->marked = 0;
			*connpoint_stack_index = (stack_top > 0) ? stack_top - 1 : -1;
			connection_point = (stack_top > 0) ? node_stack[stack_top - 1] : NULL;
		} else {
			new_node = new node_t(key, value);
			*connpoint_stack_index = (stack_top >= 0) ? stack_top : -1;
			connection_point = (stack_top >= 0) ? node_stack[stack_top] : NULL;
		}

		*privcopy = (void *)new_node;
		return (void *)connection_point;
	}

	void *delete_with_copy(const K& key, void **stack, int *unused,
	                       int *_stack_top, void **privcopy,
	                       int *connpoint_stack_index)
	{
		int stack_top = *_stack_top;
		node_t **node_stack = (node_t **)stack;

		node_t *connection_point, *tree_copy_root;
		node_t *original_to_be_deleted = node_stack[stack_top];
		node_t *to_be_deleted;
		int to_be_deleted_stack_index = stack_top, i;

		assert(stack_top >= 0);

		node_t *n = node_stack[stack_top];
		node_t *l = n->left;
		node_t *r = n->right;
		ht_insert(tdata->ht, &n->left, l);
		ht_insert(tdata->ht, &n->right, r);

		if (l != NULL && r != NULL) {
			node_t *new_node = node_new_copy(n);
			new_node->marked = 1;
			tree_copy_root = new_node;
		} else {
			tree_copy_root = l != NULL ? l : r;
		}

		*connpoint_stack_index	= (stack_top > 0) ? stack_top - 1 : -1;
		connection_point = (stack_top > 0) ? node_stack[stack_top-1] : NULL;
		*privcopy = (void *)tree_copy_root;

		return (void *)connection_point;
	}

private:


	/**
	 * Traverses the tree `bst` as dictated by `key`.
	 * When returning, `leaf` is either NULL (key not found) or the leaf that
	 * contains `key`. `parent` is either leaf's parent (if `leaf` != NULL) or
	 * the node that will be the parent of the inserted node.
	 **/
	inline void traverse(const K& key, node_t **parent, node_t **leaf)
	{
		*parent = NULL;
		*leaf = root;
	
		while (*leaf) {
			K leaf_key = (*leaf)->key;
			if (leaf_key == key)
				return;
	
			*parent = *leaf;
			*leaf = (key < leaf_key) ? (*leaf)->left : (*leaf)->right;
		}
	}
	
	const V lookup_helper(const K& key)
	{
		node_t *parent, *leaf;
	
		traverse(key, &parent, &leaf);
		if (leaf != NULL && !leaf->marked) return leaf->value;
		else return this->NO_VALUE;
	}

	const V insert_helper(const K& key, const V& value)
	{
		node_t *parent, *leaf;
	
		traverse(key, &parent, &leaf);
	
		// Empty tree case
		if (!parent && !leaf) {
			root = new node_t(key, value);
			return this->NO_VALUE;
		}
	
		// Key there in a marked node. Just unmark it.
		if (leaf && leaf->marked) {
			leaf->marked = 0;
			return this->NO_VALUE;
		}
	
		// Key already in the tree.
		if (leaf) return leaf->value;
	
		if (key < parent->key) parent->left = new node_t(key, value);
		else                   parent->right = new node_t(key, value);
	
		return this->NO_VALUE;
	}

	const V delete_helper(const K& key)
	{
		node_t *parent, *leaf;
	
		traverse(key, &parent, &leaf);
	
		// Key not in the tree (also includes empty tree case).
		if (!leaf) return this->NO_VALUE;
	
		// Key is also deleted (leaf is marked)
		if (leaf->marked) return this->NO_VALUE;
	
		if (!leaf->left) {
			if (!parent) root = leaf->right;
			else if (parent->left == leaf) parent->left = leaf->right;
			else if (parent->right == leaf) parent->right = leaf->right;
		} else if (!leaf->right) {
			if (!parent) root = leaf->left;
			else if (parent->left == leaf) parent->left = leaf->left;
			else if (parent->right == leaf) parent->right = leaf->left;
		} else { // Leaf has two children.
			leaf->marked = 1;
		}
	
		return leaf->value;
	}

	int total_paths, total_nodes, bst_violations;
	int min_path_len, max_path_len;
	int marked_nodes;
	void validate_rec(node_t *root, int _th)
	{
		if (!root)
			return;
	
		if (root->marked)
			marked_nodes++;
	
		node_t *left = root->left;
		node_t *right = root->right;
	
		total_nodes++;
		_th++;
	
		/* BST violation? */
		if (left && left->key >= root->key)
			bst_violations++;
		if (right && right->key <= root->key)
			bst_violations++;
	
		/* We found a path (a node with at least one NULL child). */
		if (!left || !right) {
			total_paths++;
	
			if (_th <= min_path_len)
				min_path_len = _th;
			if (_th >= max_path_len)
				max_path_len = _th;
		}
	
		/* Check subtrees. */
		if (left)
			validate_rec(left, _th);
		if (right)
			validate_rec(right, _th);
	}
	
	inline int validate_helper(bool print)
	{
		int check_bst = 0;
		total_paths = 0;
		min_path_len = 99999999;
		max_path_len = -1;
		total_nodes = 0;
		bst_violations = 0;
		marked_nodes = 0;
	
		validate_rec(root, 0);
	
		check_bst = (bst_violations == 0);
	
		if (print) {
			printf("Validation:\n");
			printf("=======================\n");
			printf("  BST Violation: %s\n",
			       check_bst ? "No [OK]" : "Yes [ERROR]");
			printf("  Tree size (UnMarked / Marked): %8d / %8d\n", total_nodes - marked_nodes, marked_nodes);
			printf("  Total paths: %d\n", total_paths);
			printf("  Min/max paths length: %d/%d\n", min_path_len, max_path_len);
			printf("\n");
		}
	
		return check_bst;
	}

	/*********************    FOR DEBUGGING ONLY    *******************************/
	void print_rec(node_t *root, int level)
	{
		int i;
	
		if (root)
			print_rec(root->right, level + 1);
	
		for (i = 0; i < level; i++)
			printf("|--");
	
		if (!root) {
			printf("NULL\n");
			return;
		}
	
		if (root->marked) printf("[%llu]\n", root->key);
		else              printf("%llu\n", root->key);
	
		print_rec(root->left, level + 1);
	}
	
	void print_helper()
	{
		if (root == NULL) printf("[empty]");
		else              print_rec(root, 0);
		printf("\n");
	}
	/******************************************************************************/

};

#define BST_UNB_PEXT_TEMPL template<typename K, typename V>
#define BST_UNB_PEXT_FUNCT bst_unb_pext<K,V>

BST_UNB_PEXT_TEMPL
bool BST_UNB_PEXT_FUNCT::contains(const int tid, const K& key)
{
	const V ret = lookup_helper(key);
	return ret != this->NO_VALUE;
}

BST_UNB_PEXT_TEMPL
const std::pair<V,bool> BST_UNB_PEXT_FUNCT::find(const int tid, const K& key)
{
	const V ret = lookup_helper(key);
	return std::pair<V,bool>(ret, ret != this->NO_VALUE);
}

BST_UNB_PEXT_TEMPL
int BST_UNB_PEXT_FUNCT::rangeQuery(const int tid, const K& lo, const K& hi,
                      std::vector<std::pair<K,V>> kv_pairs)
{
	return 0;
}

BST_UNB_PEXT_TEMPL
const V BST_UNB_PEXT_FUNCT::insert(const int tid, const K& key, const V& val)
{
	return insertIfAbsent(tid, key, val);
}

BST_UNB_PEXT_TEMPL
const V BST_UNB_PEXT_FUNCT::insertIfAbsent(const int tid, const K& key, const V& val)
{
	return insert_helper(key, val);
}

BST_UNB_PEXT_TEMPL
const std::pair<V,bool> BST_UNB_PEXT_FUNCT::remove(const int tid, const K& key)
{
	const V ret = delete_helper(key);
	return std::pair<V,bool>(ret, ret != this->NO_VALUE);
}

BST_UNB_PEXT_TEMPL
bool BST_UNB_PEXT_FUNCT::validate()
{
	return validate(true);
}
