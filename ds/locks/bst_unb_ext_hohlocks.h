/**
 * An external (unbalanced) binary search tree that uses fine-grained locks
 **/

#pragma once

#include "../map_if.h"
#include "Log.h"
#include "lock.h"

#define IS_EXTERNAL_NODE(node) \
    ( (node)->left == NULL && (node)->right == NULL )

template <typename K, typename V>
class bst_unb_ext_hohlocks : public Map<K,V> {
public:
	bst_unb_ext_hohlocks(const K _NO_KEY, const V _NO_VALUE, const int numProcesses)
	{
		root = NULL;
		INIT_LOCK(&root_lock);
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
	char *name() { return "BST Unbalanced External (Hand-over-hand locking)"; }

	void print() { print_helper(); };
	unsigned long long size() { return 0; };

private:

	struct node_t {
		K key;
		V value;

		node_lock_t lock;
		node_t *left,
		       *right;

		node_t(const K& key, const V& value) {
			this->key = key;
			this->value = value;
			this->left = this->right = NULL;
			INIT_LOCK(&lock);
		}
	};

	node_t *root;
	node_lock_t root_lock;

private:
	/**
	 * Traverses the tree `bst` as dictated by `key`.
	 * When returning, `leaf` is either NULL (empty tree) or the last node in the
	 * access path. `parent` is either leaf's parent (if `leaf` != NULL) or
	 * NULL.
	 * Upon return every NON-NULL pointer among `*gparent`, `*parent` and `*leaf`
	 * is locked.
	 **/
	inline void traverse(const K& key, node_t **gparent, node_t **parent, node_t **leaf)
	{
		*gparent = NULL;
		*parent = NULL;
	
		LOCK(&root_lock);
		*leaf = root;
	
		if (*leaf == NULL)
			return;
		
		LOCK(&(*leaf)->lock);
		while (!IS_EXTERNAL_NODE(*leaf)) {
			if (!*gparent && *parent) UNLOCK(&root_lock);
			if (*gparent) UNLOCK(&(*gparent)->lock);
	
			int leaf_key = (*leaf)->key;
			*gparent = *parent;
			*parent = *leaf;
			*leaf = (key <= leaf_key) ? (*leaf)->left : (*leaf)->right;
			LOCK(&(*leaf)->lock);
		}
	}
	
	int lookup_helper(const K& key)
	{
		node_t *gparent, *parent, *leaf;
	
		traverse(key, &gparent, &parent, &leaf);
		int ret = (leaf && leaf->key == key);
		if (leaf) UNLOCK(&leaf->lock);
		if (parent) UNLOCK(&parent->lock);
		if (gparent) UNLOCK(&gparent->lock);
		if (!gparent || !parent) UNLOCK(&root_lock);
		return ret;
	}
	
	int insert_helper(const K& key, const V& value)
	{
		node_t *gparent, *parent, *leaf;
	
		traverse(key, &gparent, &parent, &leaf);
	
		// Empty tree case
		if (!leaf) {
			root = new node_t(key, value);
			UNLOCK(&root_lock);
			return 1;
		}
	
		// Key already in the tree.
		if (leaf->key == key) {
			if (leaf) UNLOCK(&leaf->lock);
			if (parent) UNLOCK(&parent->lock);
			if (gparent) UNLOCK(&gparent->lock);
			if (!gparent || !parent) UNLOCK(&root->lock);
			return 0;
		}
	
		// Create new internal and leaf nodes.
		node_t *new_internal = new node_t(key, NULL);
		if (key <= leaf->key) {
			new_internal->left = new node_t(key, value);
			new_internal->right = leaf;
		} else {
			new_internal->left = leaf;
			new_internal->right = new node_t(key, value);
			new_internal->key = leaf->key;
		}
	
		if (!parent) {
			root = new_internal;
			UNLOCK(&leaf->lock);
			UNLOCK(&root_lock);
		} else {
			if (key <= parent->key) parent->left = new_internal;
			else                    parent->right = new_internal;
			if (gparent) UNLOCK(&gparent->lock);
			else         UNLOCK(&root_lock);
			UNLOCK(&parent->lock);
			UNLOCK(&leaf->lock);
		}
	
		return 1;
	}
	
	int delete_helper(const K& key)
	{
		node_t *gparent, *parent, *leaf;
	
		traverse(key, &gparent, &parent, &leaf);
	
		// Empty tree.
		if (!leaf) {
			UNLOCK(&root_lock);
			return 0;
		}
		// Key not in the tree.
		if (leaf->key != key) {
			if (leaf) UNLOCK(&leaf->lock);
			if (parent) UNLOCK(&parent->lock);
			if (gparent) UNLOCK(&gparent->lock);
			if (!gparent || !parent) UNLOCK(&root_lock);
			return 0;
		}
	
		// Only one node in the tree.
		if (!parent) {
			root = NULL;
			UNLOCK(&leaf->lock);
			UNLOCK(&root_lock);
			return 1;
		}
	
		node_t *sibling = (key <= parent->key) ? parent->right : parent->left;
		if (!gparent) {
			UNLOCK(&leaf->lock);
			UNLOCK(&parent->lock);
			UNLOCK(&root_lock);
			root = sibling;
		} else {
			if (key <= gparent->key) gparent->left = sibling;
			else                     gparent->right = sibling;
			UNLOCK(&leaf->lock);
			UNLOCK(&parent->lock);
			UNLOCK(&gparent->lock);
		}
	
		return 1;
	}
	
	int key_in_max_path, key_in_min_path;
	int total_paths, total_nodes, bst_violations;
	int min_path_len, max_path_len;
	void validate_rec(node_t *root, int _th)
	{
		if (!root) return;
	
		node_t *left = root->left;
		node_t *right = root->right;
	
		total_nodes++;
		_th++;
	
		/* BST violation? */
		if (left && left->key > root->key)
			bst_violations++;
		if (right && right->key <= root->key)
			bst_violations++;
	
		/* We found a path (a node with at least one NULL child). */
		if (IS_EXTERNAL_NODE(root)) {
			total_paths++;
	
			if (_th <= min_path_len){
				min_path_len = _th;
				key_in_min_path = root->key;
			}
			if (_th >= max_path_len){
				max_path_len = _th;
				key_in_max_path = root->key;
			}
		}
	
		/* Check subtrees. */
		if (left)  validate_rec(left, _th);
		if (right) validate_rec(right, _th);
	}
	
	int validate_helper()
	{
		int check_bst = 0;
		total_paths = 0;
		min_path_len = 99999999;
		max_path_len = -1;
		total_nodes = 0;
		bst_violations = 0;
	
		validate_rec(root, 0);
	
		check_bst = (bst_violations == 0);
	
		printf("Validation:\n");
		printf("=======================\n");
		printf("  BST Violation: %s\n",
		       check_bst ? "No [OK]" : "Yes [ERROR]");
		printf("  Tree size: %8d\n", total_nodes);
		printf("  Total paths: %d\n", total_paths);
		printf("  Min/max paths length: %d/%d\n", min_path_len, max_path_len);
		printf("  Key of min path: %d\n", key_in_min_path);
		printf("  Key of max path: %d\n", key_in_max_path);
		printf("\n");
	
		return check_bst;
	}
	
	/*********************    FOR DEBUGGING ONLY    *******************************/
	void print_rec(node_t *root, int level)
	{
		if (root) print_rec(root->right, level + 1);
	
		for (int i = 0; i < level; i++) printf("|--");
	
		if (!root) {
			printf("NULL\n");
			return;
		}
	
		printf("%d\n", root->key);
	
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

#define BST_UNB_EXT_HOHLOCKS_TEMPL template<typename K, typename V>
#define BST_UNB_EXT_HOHLOCKS_FUNCT bst_unb_ext_hohlocks<K,V>

BST_UNB_EXT_HOHLOCKS_TEMPL
bool BST_UNB_EXT_HOHLOCKS_FUNCT::contains(const int tid, const K& key)
{
	return lookup_helper(key);
}

BST_UNB_EXT_HOHLOCKS_TEMPL
const std::pair<V,bool> BST_UNB_EXT_HOHLOCKS_FUNCT::find(const int tid, const K& key)
{
	int ret = lookup_helper(key);
	return std::pair<V,bool>(NULL, ret);
}

BST_UNB_EXT_HOHLOCKS_TEMPL
int BST_UNB_EXT_HOHLOCKS_FUNCT::rangeQuery(const int tid, const K& lo, const K& hi,
                      std::vector<std::pair<K,V>> kv_pairs)
{
	return 0;
}

BST_UNB_EXT_HOHLOCKS_TEMPL
const V BST_UNB_EXT_HOHLOCKS_FUNCT::insert(const int tid, const K& key, const V& val)
{
	return insertIfAbsent(tid, key, val);
}

BST_UNB_EXT_HOHLOCKS_TEMPL
const V BST_UNB_EXT_HOHLOCKS_FUNCT::insertIfAbsent(const int tid, const K& key, const V& val)
{
	int ret = insert_helper(key, val);
	if (ret == 1) return NULL;
	else return (void *)1;
}

BST_UNB_EXT_HOHLOCKS_TEMPL
const std::pair<V,bool> BST_UNB_EXT_HOHLOCKS_FUNCT::remove(const int tid, const K& key)
{
	int ret = delete_helper(key);
	if (ret == 0) return std::pair<V,bool>(NULL, false);
	else return std::pair<V,bool>((void*)1, true);
}

BST_UNB_EXT_HOHLOCKS_TEMPL
bool BST_UNB_EXT_HOHLOCKS_FUNCT::validate()
{
	return validate_helper();
}
