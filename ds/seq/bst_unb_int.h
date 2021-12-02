/**
 * An internal (unbalanced) binary search tree.
 **/

#pragma once

#include "../rcu-htm/ht.h"
#include "../map_if.h"
#include "Log.h"

template <typename K, typename V>
class bst_unb_int : public Map<K,V> {
public:
	bst_unb_int(const K _NO_KEY, const V _NO_VALUE, const int numProcesses)
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
	char *name() { return "BST Unbalanced Internal"; }

	void print();
	unsigned long long size() { return size_rec(root); };

private:

	typedef bst_unb_int<K, V> tree_t;

	struct node_t {
		K key;
		V value;
		node_t *left, *right;

		node_t (const K& key_, const V& value_) :
		     key(key_), value(value_),
		     left(NULL), right(NULL) {};
	};

	inline node_t *node_new_copy(node_t *src)
	{
		node_t *ret = new node_t(src->key, src->value);
		ret->left = src->left;
		ret->right = src->right;
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
		return get_key_sum_rec(n->left) + (long long)n->key + get_key_sum_rec(n->right);
	}

	bool validate(bool print) { return validate_helper(print); };

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
			leaf = (key < leaf->key) ? leaf->left : leaf->right;
		}

		if (*stack_top >= 0 && node_stack[*stack_top]->key == key)
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
		if (stack_top >= 0 && node_stack_indexes[stack_top] == 0 && node_stack[stack_top]->left != NULL)
			TX_ABORT(ABORT_VALIDATION_FAILURE);
		if (stack_top >= 0 && node_stack_indexes[stack_top] == 1 && node_stack[stack_top]->right != NULL)
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
		node_t *new_node, *connection_point;
	
		// Create new node.
		new_node = new node_t(key, value);
	
		*connpoint_stack_index = (stack_top >= 0) ? stack_top : -1;
		connection_point = (stack_top >= 0) ? node_stack[stack_top] : NULL;
	
		*privcopy = (void *)new_node;
		return (void *)connection_point;
	}

	void find_successor_with_stack(node_t *node, node_t **node_stack,
	                               int *node_stack_indexes, int *stack_top)
	{
		node_t *parent, *leaf, *l, *r;
	
		l = node->left;
		r = node->right;
		ht_insert(tdata->ht, &node->left, l);
		ht_insert(tdata->ht, &node->right, r);
		if (!l || !r)
			return;
	
		parent = node;
		leaf = r;
		node_stack_indexes[*stack_top] = 1;
		node_stack[++(*stack_top)] = leaf;
		node_stack_indexes[*stack_top] = 0;
	
		while ((l = leaf->left) != NULL) {
			ht_insert(tdata->ht, &leaf->left, l);
			parent = leaf;
			leaf = l;
			node_stack[++(*stack_top)] = leaf;
			node_stack_indexes[*stack_top] = 0;
		}
		ht_insert(tdata->ht, &leaf->left, NULL);
	}
	void *delete_with_copy(const K& key, void **stack, int *node_stack_indexes,
	                       int *_stack_top, void **privcopy,
	                       int *connpoint_stack_index)
	{
		int stack_top = *_stack_top;
		node_t **node_stack = (node_t **)stack;

		node_t *tree_copy_root, *connection_point;
		node_t *original_to_be_deleted = node_stack[stack_top];
		node_t *to_be_deleted;
		int to_be_deleted_stack_index = stack_top, i;
	
		assert(stack_top >= 0);
	
		find_successor_with_stack(original_to_be_deleted, node_stack,
		                          node_stack_indexes, &stack_top);
		*_stack_top = stack_top;
		to_be_deleted = node_stack[stack_top];
	
		node_t *l = to_be_deleted->left;
		node_t *r = to_be_deleted->right;
		ht_insert(tdata->ht, &to_be_deleted->left, l);
		ht_insert(tdata->ht, &to_be_deleted->right, r);
		tree_copy_root = l != NULL ? l : r;
		*connpoint_stack_index = (stack_top > 0) ? stack_top - 1 : -1;
		connection_point = (stack_top > 0) ? node_stack[stack_top-1] : NULL;
	
		// We may need to copy the access path from the originally deleted node
		// up to the current connection_point.
		if (to_be_deleted_stack_index <= *connpoint_stack_index) {
			for (i=*connpoint_stack_index; i >= to_be_deleted_stack_index; i--) {
				node_t *curr_cp = node_new_copy(node_stack[i]);
				ht_insert(tdata->ht, &node_stack[i]->left, curr_cp->left);
				ht_insert(tdata->ht, &node_stack[i]->right, curr_cp->right);
	
				if (key < curr_cp->key) curr_cp->left = tree_copy_root;
				else                    curr_cp->right = tree_copy_root;
				tree_copy_root = curr_cp;
			}
			tree_copy_root->key = to_be_deleted->key;
			tree_copy_root->value = to_be_deleted->value;
			connection_point = to_be_deleted_stack_index > 0 ? 
			                            node_stack[to_be_deleted_stack_index - 1] :
			                            NULL;
			*connpoint_stack_index = to_be_deleted_stack_index - 1;
		}
	
		*privcopy = (void *)tree_copy_root;
		return (void *)connection_point;
	}

private:

	inline void traverse(const K& key, node_t **parent, node_t **leaf);
	inline void find_successor(node_t *node, node_t **parent, node_t **leaf);

	const V lookup_helper(const K& key);
	const V insert_helper(const K& key, const V& value);
	const V delete_helper(const K& key);
	int update_helper(const K& key, const V& value);

	int validate_helper(bool print);
	void validate_rec(node_t *root, int _th);

	void print_rec(node_t *root, int level);
	unsigned long long size_rec(node_t *root);
};

#define TEMPL template<typename K, typename V>
#define FUNCT bst_unb_int<K,V>

TEMPL
bool FUNCT::contains(const int tid, const K& key)
{
	const V ret = lookup_helper(key);
	return ret != this->NO_VALUE;
}

TEMPL
const std::pair<V,bool> FUNCT::find(const int tid, const K& key)
{
	const V ret = lookup_helper(key);
	return std::pair<V,bool>(ret, ret != this->NO_VALUE);
}

TEMPL
int FUNCT::rangeQuery(const int tid, const K& lo, const K& hi,
                      std::vector<std::pair<K,V>> kv_pairs)
{
	return 0;
}

TEMPL
const V FUNCT::insert(const int tid, const K& key, const V& val)
{
	return insertIfAbsent(tid, key, val);
}

TEMPL
const V FUNCT::insertIfAbsent(const int tid, const K& key, const V& val)
{
	return insert_helper(key, val);
}

TEMPL
const std::pair<V,bool> FUNCT::remove(const int tid, const K& key)
{
	const V ret = delete_helper(key);
	return std::pair<V,bool>(ret, ret != this->NO_VALUE);
}

TEMPL
bool FUNCT::validate()
{
	return validate(true);
}


/**
 * Traverses the tree `bst` as dictated by `key`.
 * When returning, `leaf` is either NULL (key not found) or the leaf that
 * contains `key`. `parent` is either leaf's parent (if `leaf` != NULL) or
 * the node that will be the parent of the inserted node.
 **/
TEMPL
inline void FUNCT::traverse(const K& key, node_t **parent, node_t **leaf)
{
	*parent = NULL;
	*leaf = root;

	while (*leaf) {
		if ((*leaf)->key == key) return;

		*parent = *leaf;
		*leaf = (key < (*leaf)->key) ? (*leaf)->left : (*leaf)->right;
	}
}

TEMPL
const V FUNCT::lookup_helper(const K& key)
{
	node_t *parent, *leaf;
	traverse(key, &parent, &leaf);
	if (leaf != NULL) return leaf->value;
	else return this->NO_VALUE;
}

TEMPL
const V FUNCT::insert_helper(const K& key, const V& value)
{
	node_t *parent, *leaf;

	traverse(key, &parent, &leaf);

	// Empty tree case
	if (!parent && !leaf) {
		root = new node_t(key, value);
		return this->NO_VALUE;
	}

	// Key already in the tree.
	if (leaf) return leaf->value;

	if (key < parent->key) parent->left =  new node_t(key, value);
	else                   parent->right = new node_t(key, value);

	return this->NO_VALUE;
}

TEMPL
inline void FUNCT::find_successor(node_t *node, node_t **parent, node_t **leaf)
{
	*parent = node;
	*leaf = node->right;

	while ((*leaf)->left) {
		*parent = *leaf;
		*leaf = (*leaf)->left;
	}
}

TEMPL
const V FUNCT::delete_helper(const K& key)
{
	node_t *parent, *leaf, *succ, *succ_parent;

	traverse(key, &parent, &leaf);

	// Key not in the tree (also includes empty tree case).
	if (!leaf) return this->NO_VALUE;

	const V del_val = leaf->value;

	if (!leaf->left) {
		if (!parent) root = leaf->right;
		else if (parent->left == leaf) parent->left = leaf->right;
		else if (parent->right == leaf) parent->right = leaf->right;
	} else if (!leaf->right) {
		if (!parent) root = leaf->left;
		else if (parent->left == leaf) parent->left = leaf->left;
		else if (parent->right == leaf) parent->right = leaf->left;
	} else { // Leaf has two children.
		find_successor(leaf, &succ_parent, &succ);

		leaf->key = succ->key;
		leaf->value = succ->value;
		if (succ_parent->left == succ) succ_parent->left = succ->right;
		else succ_parent->right = succ->right;
	}

	return del_val;
}

TEMPL
int FUNCT::update_helper(const K& key, const V& value)
{
	node_t *parent, *leaf, *succ, *succ_parent;

	traverse(key, &parent, &leaf);

	// Empty tree case
	if (!parent && !leaf) {
//		root = bst_node_new(key, value);
		root = new node_t(key, value);
		return 1;
	}

	// Insertion
	if (!leaf) {
//		if (key < parent->key) parent->left = bst_node_new(key, value);
//		else                   parent->right = bst_node_new(key, value);
		if (key < parent->key) parent->left = new node_t(key, value);
		else                   parent->right = new node_t(key, value);
		return 1;
	}

	//> Deletion
	if (!leaf->left) {
		if (!parent) root = leaf->right;
		else if (parent->left == leaf) parent->left = leaf->right;
		else if (parent->right == leaf) parent->right = leaf->right;
	} else if (!leaf->right) {
		if (!parent) root = leaf->left;
		else if (parent->left == leaf) parent->left = leaf->left;
		else if (parent->right == leaf) parent->right = leaf->left;
	} else { // Leaf has two children.
		find_successor(leaf, &succ_parent, &succ);
		leaf->key = succ->key;
		if (succ_parent->left == succ) succ_parent->left = succ->right;
		else succ_parent->right = succ->right;
	}

	return 3;
}

TEMPL
void FUNCT::print_rec(node_t *root, int level)
{
	int i;

	if (root) print_rec(root->right, level + 1);

	for (i = 0; i < level; i++) printf("|--");

	if (!root) {
		printf("|~\n");
		return;
	}

//	KEY_PRINT(root->key, "", "\n");
	std::cout << root->key << std::endl;

	print_rec(root->left, level + 1);
}

TEMPL
void FUNCT::print()
{
	if (root == NULL) log_info("[empty]");
	else              print_rec(root, 0);
	log_info("\n");
}

TEMPL
unsigned long long FUNCT::size_rec(node_t *root)
{
	if (root == NULL) return 0;
	else return size_rec(root->left) + 1 + size_rec(root->right);
}

//static K key_in_max_path, key_in_min_path;
static int total_paths, total_nodes, bst_violations;
static int min_path_len, max_path_len;

TEMPL
void FUNCT::validate_rec(node_t *root, int _th)
{
	if (!root) return;

	node_t *left = root->left;
	node_t *right = root->right;

	total_nodes++;
	_th++;

	/* BST violation? */
	if (left &&  left->key >= root->key)   bst_violations++;
	if (right && right->key <= root->key) bst_violations++;

	/* We found a path (a node with at least one NULL child). */
	if (!left || !right) {
		total_paths++;

		if (_th <= min_path_len){
			min_path_len = _th;
//			key_in_min_path = root->key;
		}
		if (_th >= max_path_len){
			max_path_len = _th;
//			key_in_max_path = root->key;
		}
	}

	/* Check subtrees. */
	if (left)  validate_rec(left, _th);
	if (right) validate_rec(right, _th);
}

TEMPL
int FUNCT::validate_helper(bool print)
{
	int check_bst = 0;
	total_paths = 0;
	min_path_len = 99999999;
	max_path_len = -1;
	total_nodes = 0;
	bst_violations = 0;

	validate_rec(root, 0);

	check_bst = (bst_violations == 0);

	if (print) {
		log_info("Validation:\n");
		log_info("=======================\n");
		log_info("  BST Violation: %s\n",
		         check_bst ? "No [OK]" : "Yes [ERROR]");
		log_info("  Tree size: %8d\n", total_nodes);
		log_info("  Total paths: %d\n", total_paths);
		log_info("  Min/max paths length: %d/%d\n", min_path_len, max_path_len);
//		KEY_PRINT(key_in_min_path, "  Key of min path: ", "\n");
//		KEY_PRINT(key_in_max_path, "  Key of max path: ", "\n");
		log_info("\n");
	}

	return check_bst;
}
