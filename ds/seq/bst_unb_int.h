/**
 * An internal (unbalanced) binary search tree.
 **/

#pragma once

#include "../map_if.h"
#include "Log.h"

template <typename K, typename V>
class bst_unb_int : public Map<K,V> {
public:
	bst_unb_int(const K _NO_KEY, const V _NO_VALUE, const int numProcesses)
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

	struct node_t {
		K key;
		V value;
		node_t *left, *right;

		node_t (const K& key_, const V& value_) :
		     key(key_), value(value_),
		     left(NULL), right(NULL) {};
	};

	node_t *root;

private:

	inline void traverse(const K& key, node_t **parent, node_t **leaf);
	inline void find_successor(node_t *node, node_t **parent, node_t **leaf);

	int lookup_helper(const K& key);
	int insert_helper(const K& key, const V& value);
	int delete_helper(const K& key);
	int update_helper(const K& key, const V& value);

	int validate_helper();
	void validate_rec(node_t *root, int _th);

	void print_rec(node_t *root, int level);
	unsigned long long size_rec(node_t *root);
};

#define TEMPL template<typename K, typename V>
#define FUNCT bst_unb_int<K,V>

TEMPL
bool FUNCT::contains(const int tid, const K& key)
{
	return lookup_helper(key);
}

TEMPL
const std::pair<V,bool> FUNCT::find(const int tid, const K& key)
{
	return std::pair<V,bool>(NULL, false);
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
	int ret = insert_helper(key, val);
	if (ret == 1) return NULL;
	else return (void *)1;
}

TEMPL
const std::pair<V,bool> FUNCT::remove(const int tid, const K& key)
{
	int ret = delete_helper(key);
	if (ret == 0) return std::pair<V,bool>(NULL, false);
	else return std::pair<V,bool>((void*)1, true);
}

TEMPL
bool FUNCT::validate()
{
	return validate_helper();
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
int FUNCT::lookup_helper(const K& key)
{
	node_t *parent, *leaf;
	traverse(key, &parent, &leaf);
	return (leaf != NULL);
}

TEMPL
int FUNCT::insert_helper(const K& key, const V& value)
{
	node_t *parent, *leaf;

	traverse(key, &parent, &leaf);

	// Empty tree case
	if (!parent && !leaf) {
//		root = bst_node_new(key, value);
		root = new node_t(key, value);
		return 1;
	}

	// Key already in the tree.
	if (leaf) return 0;

//	if (key < parent->key) parent->left = bst_node_new(key, value);
//	else                       parent->right = bst_node_new(key, value);
	if (key < parent->key) parent->left =  new node_t(key, value);
	else                   parent->right = new node_t(key, value);

	return 1;
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
int FUNCT::delete_helper(const K& key)
{
	node_t *parent, *leaf, *succ, *succ_parent;

	traverse(key, &parent, &leaf);

	// Key not in the tree (also includes empty tree case).
	if (!leaf) return 0;

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

	return 1;
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
int FUNCT::validate_helper()
{
	int check_bst = 0;
	total_paths = 0;
	min_path_len = 99999999;
	max_path_len = -1;
	total_nodes = 0;
	bst_violations = 0;

	validate_rec(root, 0);

	check_bst = (bst_violations == 0);

	log_info("Validation:\n");
	log_info("=======================\n");
	log_info("  BST Violation: %s\n",
	         check_bst ? "No [OK]" : "Yes [ERROR]");
	log_info("  Tree size: %8d\n", total_nodes);
	log_info("  Total paths: %d\n", total_paths);
	log_info("  Min/max paths length: %d/%d\n", min_path_len, max_path_len);
//	KEY_PRINT(key_in_min_path, "  Key of min path: ", "\n");
//	KEY_PRINT(key_in_max_path, "  Key of max path: ", "\n");
	log_info("\n");

	return check_bst;
}
