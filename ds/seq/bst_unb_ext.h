/**
 * An external (unbalanced) binary search tree.
 **/

#pragma once

#include "../map_if.h"
#include "Log.h"

template <typename K, typename V>
class bst_unb_ext : public Map<K,V> {
public:
	bst_unb_ext(const K _NO_KEY, const V _NO_VALUE, const int numProcesses)
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
	char *name() { return "BST Unbalanced External"; }

	void print();
	unsigned long long size() { return size_rec(root); };

private:

	struct node_t {

		#define IS_EXTERNAL_NODE(node) \
		        ( (node)->left == NULL && (node)->right == NULL )

		K key;
		V value;
		node_t *left, *right;

		node_t (const K& key_, const V& value_) :
		     key(key_), value(value_),
		     left(NULL), right(NULL) {};
	};

	node_t *root;

private:

	inline void traverse(const K& key, node_t **gparent, node_t **parent, node_t **leaf);
	inline void find_successor(node_t *node, node_t **parent, node_t **leaf);

	int lookup_helper(const K& key);
	int insert_helper(const K& key, const V& value);
	int delete_helper(const K& key);
	int update_helper(const K& key, const V& value);

	int validate_helper();
	void validate_rec(node_t *root, int _th);

	void print_rec(node_t *root, int level);
	unsigned long long size_rec(node_t *root);

private:

	//static K key_in_max_path, key_in_min_path;
	int total_paths, total_nodes, bst_violations;
	int min_path_len, max_path_len;

};

#define BST_UNB_EXT_TEMPL template<typename K, typename V>
#define BST_UNB_EXT_FUNCT bst_unb_ext<K,V>

BST_UNB_EXT_TEMPL
bool BST_UNB_EXT_FUNCT::contains(const int tid, const K& key)
{
	return lookup_helper(key);
}

BST_UNB_EXT_TEMPL
const std::pair<V,bool> BST_UNB_EXT_FUNCT::find(const int tid, const K& key)
{
	return std::pair<V,bool>(NULL, false);
}

BST_UNB_EXT_TEMPL
int BST_UNB_EXT_FUNCT::rangeQuery(const int tid, const K& lo, const K& hi,
                      std::vector<std::pair<K,V>> kv_pairs)
{
	return 0;
}

BST_UNB_EXT_TEMPL
const V BST_UNB_EXT_FUNCT::insert(const int tid, const K& key, const V& val)
{
	return insertIfAbsent(tid, key, val);
}

BST_UNB_EXT_TEMPL
const V BST_UNB_EXT_FUNCT::insertIfAbsent(const int tid, const K& key, const V& val)
{
	int ret = insert_helper(key, val);
	if (ret == 1) return NULL;
	else return (void *)1;
}

BST_UNB_EXT_TEMPL
const std::pair<V,bool> BST_UNB_EXT_FUNCT::remove(const int tid, const K& key)
{
	int ret = delete_helper(key);
	if (ret == 0) return std::pair<V,bool>(NULL, false);
	else return std::pair<V,bool>((void*)1, true);
}

BST_UNB_EXT_TEMPL
bool BST_UNB_EXT_FUNCT::validate()
{
	return validate_helper();
}

/**
 * Traverses the tree `bst` as dictated by `key`.
 * When returning, `leaf` is either NULL (empty tree) or the last node in the
 * access path. `parent` is either leaf's parent (if `leaf` != NULL) or
 * NULL.
 **/
BST_UNB_EXT_TEMPL
inline void BST_UNB_EXT_FUNCT::traverse(const K& key, node_t **gparent,
                                        node_t **parent, node_t **leaf)
{
	*gparent = NULL;
	*parent = NULL;
	*leaf = root;

	if (*leaf == NULL) return;

	while (!IS_EXTERNAL_NODE(*leaf)) {
		*gparent = *parent;
		*parent = *leaf;
		*leaf = (key <= (*leaf)->key) ? (*leaf)->left : (*leaf)->right;
	}
}

BST_UNB_EXT_TEMPL
int BST_UNB_EXT_FUNCT::lookup_helper(const K& key)
{
	node_t *gparent, *parent, *leaf;
	traverse(key, &gparent, &parent, &leaf);
	return (leaf && leaf->key == key);
}

BST_UNB_EXT_TEMPL
int BST_UNB_EXT_FUNCT::insert_helper(const K& key, const V& value)
{
	node_t *gparent, *parent, *leaf;

	traverse(key, &gparent, &parent, &leaf);

	// Empty tree case
	if (!leaf) {
//		root = bst_node_new(key, value);
		root = new node_t(key, value);
		return 1;
	}

	// Key already in the tree.
	if (leaf->key == key)
		return 0;

	// Create new internal and leaf nodes.
//	bst_node_t *new_internal = bst_node_new(key, NULL);
	node_t *new_internal = new node_t(key, NULL);
	if (key <= leaf->key) {
//		new_internal->left = bst_node_new(key, value);
		new_internal->left = new node_t(key, value);
		new_internal->right = leaf;
	} else {
		new_internal->left = leaf;
//		new_internal->right = bst_node_new(key, value);
		new_internal->right = new node_t(key, value);
		new_internal->key = leaf->key;
	}
	if (!parent)                 root = new_internal;
	else if (key <= parent->key) parent->left = new_internal;
	else                         parent->right = new_internal;
	return 1;
}

BST_UNB_EXT_TEMPL
int BST_UNB_EXT_FUNCT::delete_helper(const K& key)
{

	node_t *gparent, *parent, *leaf;

	traverse(key, &gparent, &parent, &leaf);

	// Empty tree or key not in the tree.
	if (!leaf || leaf->key != key)
		return 0;

	// Only one node in the tree.
	if (!parent) {
		root = NULL;
		return 1;
	}
	node_t *sibling = (key <= parent->key) ? parent->right : parent->left;
	if (!gparent)                 root = sibling;
	else if (key <= gparent->key) gparent->left = sibling;
	else                          gparent->right = sibling;
	return 1;
}

BST_UNB_EXT_TEMPL
int BST_UNB_EXT_FUNCT::update_helper(const K& key, const V& value)
{
	node_t *gparent, *parent, *leaf;

	traverse(key, &gparent, &parent, &leaf);

	// Empty tree case. Insert
	if (!leaf) {
//		root = bst_node_new(key, value);
		root = new node_t(key, value);
		return 1;
	}

	// Key already in the tree. Delete
	if (leaf->key == key) {
		if (!parent) {
			root = NULL;
			return 3;
		}
		node_t *sibling = (key <= parent->key) ? parent->right : parent->left;
		if (!gparent)                 root = sibling;
		else if (key <= gparent->key) gparent->left = sibling;
		else                          gparent->right = sibling;
		return 3;
	}

	// Create new internal and leaf nodes.
//	node_t *new_internal = bst_node_new(key, NULL);
	node_t *new_internal = new node_t(key, NULL);
	if (key <= leaf->key) {
//		new_internal->left = bst_node_new(key, value);
		new_internal->left = new node_t(key, value);
		new_internal->right = leaf;
	} else {
		new_internal->left = leaf;
//		new_internal->right = bst_node_new(key, value);
		new_internal->right = new node_t(key, value);
		new_internal->key = leaf->key;
	}

	if (!parent)                 root = new_internal;
	else if (key <= parent->key) parent->left = new_internal;
	else                         parent->right = new_internal;

	return 1;
}

BST_UNB_EXT_TEMPL
void BST_UNB_EXT_FUNCT::print_rec(node_t *root, int level)
{
	if (root)
		print_rec(root->right, level + 1);

	for (int i = 0; i < level; i++)
		printf("|--");

	if (!root) {
		printf("NULL\n");
		return;
	}

//	KEY_PRINT(root->key, "", "\n");
	std::cout << root->key << std::endl;

	print_rec(root->left, level + 1);
}

BST_UNB_EXT_TEMPL
void BST_UNB_EXT_FUNCT::print()
{
	if (root == NULL) log_info("[empty]");
	else              print_rec(root, 0);
	log_info("\n");
}

BST_UNB_EXT_TEMPL
unsigned long long BST_UNB_EXT_FUNCT::size_rec(node_t *root)
{
	if (root == NULL) return 0;
	else return size_rec(root->left) + 1 + size_rec(root->right);
}

BST_UNB_EXT_TEMPL
void BST_UNB_EXT_FUNCT::validate_rec(node_t *root, int _th)
{
	if (!root) return;

	node_t *left = root->left;
	node_t *right = root->right;

	total_nodes++;
	_th++;

	/* BST violation? */
	if (left &&  left->key > root->key)   bst_violations++;
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

BST_UNB_EXT_TEMPL
int BST_UNB_EXT_FUNCT::validate_helper()
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
