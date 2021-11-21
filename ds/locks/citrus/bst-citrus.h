/**
 * An internal (unbalanced) binary search tree that uses RCU with fg-locks.
 * Paper:
 *    Concurrent updates with RCU: search tree as an example, Arbel et. al, PODC 2014
 **/

#pragma once

#include "../../map_if.h"
#include "Log.h"
#include "../lock.h"
#include "urcu.h"

template <typename K, typename V>
class bst_unb_citrus : public Map<K,V> {
public:
	bst_unb_citrus(const K _NO_KEY, const V _NO_VALUE, const int numProcesses)
	  : Map<K,V>(_NO_KEY, _NO_VALUE)
	{
		initURCU(88);
		root = new node_t(this->INF_KEY, this->NO_VALUE);
		root->left = new node_t(this->INF_KEY-1, this->NO_VALUE);
	}

	void initThread(const int tid) { urcu_register(tid); };
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
	char *name() { return "BST Unbalanced Citrus"; }

	void print() { print_struct(); };
//	unsigned long long size() { return size_rec(root); };

private:

	struct node_t {
		K key;
		V value;
		node_t *left, *right;

		bool marked;
		node_lock_t lock;

		node_t (const K& key, const V& value) {
			this->key = key;
			this->value = value;
			this->marked = false;
			this->right = this->left = NULL;
			INIT_LOCK(&this->lock);
		};
	};

	node_t *root;

private:

	/**
	 * Traverses the tree `bst` as dictated by `key`.
	 * When returning, `leaf` is either NULL (key not found) or the leaf that
	 * contains `key`. `parent` is either leaf's parent (if `leaf` != NULL) or
	 * the node that will be the parent of the inserted node.
	 **/
	void traverse(const K& key, node_t **parent, node_t **leaf)
	{
		*parent = root->left;
		*leaf = root->left->left;
	
		while (*leaf) {
			if ((*leaf)->key == key) return;
			*parent = *leaf;
			*leaf = (key < (*leaf)->key) ? (*leaf)->left : (*leaf)->right;
		}
	}
	
	const V lookup_helper(const K& key)
	{
		node_t *parent, *leaf;
	
		urcu_read_lock();
		traverse(key, &parent, &leaf);
		const V retval = (leaf == NULL) ? this->NO_VALUE : leaf->value;
		urcu_read_unlock();
		return retval;
	}

	int validate(node_t *prev, node_t *curr, int direction)
	{
		int result;     
		result = !(prev->marked);
		if (direction == 0) result = result && (prev->left == curr);
		else                result = result && (prev->right == curr);
		if (curr != NULL) result = result && (!curr->marked);
		return result;
	}

	int traverse_with_direction(const K& key, node_t **prev_p, node_t **curr_p) 
	{
		node_t *prev = root, *curr = prev->left;
	    int direction = 0;
	
		while (curr && curr->key != key) {
			prev = curr;
			if (curr->key > key) {
				curr = curr->left;
				direction = 0;
			} else {
				curr = curr->right;
				direction = 1;
			}
	
			if (curr) curr->key = curr->key;
		}
	
		*prev_p = prev;
		*curr_p = curr;
		return direction;
	}

	int do_insert(const K& key, const V& value, node_t *prev, node_t *curr,
	              int direction)
	{
		node_t *new_node;
		LOCK(&prev->lock);
		if(!validate(prev, curr, direction)) {
			UNLOCK(&prev->lock);
			return 0;
		}
		new_node = new node_t(key, value); 
		if (direction == 0) prev->left = new_node;
		else                prev->right = new_node;
		UNLOCK(&prev->lock);
		return 1;
	}

	const V insert_helper(const K& key, const V& value)
	{
		node_t *prev, *curr, *new_node;
		int direction;
	
		while(1){
			urcu_read_lock();
			direction = traverse_with_direction(key, &prev, &curr);
			urcu_read_unlock();
	
			// Key already in the tree
			if (curr != NULL) return curr->value;
	
			if (do_insert(key, value, prev, curr, direction) == 1) return this->NO_VALUE;
		}
	}

	int do_delete(node_t *prev, node_t *curr, int direction)
	{
		LOCK(&prev->lock);
		LOCK(&curr->lock);
		if(!validate(prev, curr, direction)) {
			UNLOCK(&prev->lock);
			UNLOCK(&curr->lock);
			return 0;
		}
	
		if (!curr->left) {
			curr->marked = true;
			if (direction == 0) prev->left = curr->right;
			else                prev->right = curr->right;
			UNLOCK(&prev->lock);
			UNLOCK(&curr->lock);
			return 1;
		} else if (!curr->right) {
			curr->marked = true;
			if (direction == 0) prev->left = curr->left;
			else                prev->right = curr->left;
			UNLOCK(&prev->lock);
			UNLOCK(&curr->lock);
			return 1;
		}
	
		// FIXME Ugly code below :-)
		node_t *prevSucc = curr;
		node_t *succ = curr->right; 
	        
		node_t *next = succ->left;
		while (next != NULL){
			prevSucc = succ;
			succ = next;
			next = next->left;
		}		
	
		int succDirection = 1; 
		if (prevSucc != curr){
			LOCK(&prevSucc->lock);
			succDirection = 0;
		} 		
		LOCK(&succ->lock);
		if (validate(prevSucc, succ, succDirection) && validate(succ, NULL, 0)) {
			curr->marked=1;
			node_t *new_node = new node_t(succ->key, succ->value);
			new_node->left = curr->left;
			new_node->right = curr->right;
			LOCK(&new_node->lock);
			if (direction == 0) prev->left = new_node;
			else                prev->right = new_node;
			urcu_synchronize();
			succ->marked = true;
			if (prevSucc == curr) new_node->right = succ->right;
			else                  prevSucc->left = succ->right;
			UNLOCK(&prev->lock);
			UNLOCK(&new_node->lock);
			UNLOCK(&curr->lock);
			if (prevSucc != curr) UNLOCK(&prevSucc->lock);
			UNLOCK(&succ->lock);
			return 1; 
		}
		UNLOCK(&prev->lock);
		UNLOCK(&curr->lock);
		if (prevSucc != curr) UNLOCK(&prevSucc->lock);
		UNLOCK(&succ->lock);
		return 0;
	}
	
	const V delete_helper(const K& key)
	{
		node_t *prev, *curr;
		int direction;
	
	    while(1) {
			urcu_read_lock();    
			direction = traverse_with_direction(key, &prev, &curr);
			urcu_read_unlock();
	
			// Key not found
			if (!curr) return this->NO_VALUE;

			const V del_val = curr->value;
			if (do_delete(prev, curr, direction) == 1) return del_val;
	    }
	}

//	int _bst_update_helper(bst_t *bst, int key, void *value)
//	{
//		bst_node_t *prev, *curr, *new;
//		int direction;
//		int op_is_insert = -1;
//	
//		while(1){
//			urcu_read_lock();
//			direction = _traverse_with_direction(bst, key, &prev, &curr);
//			urcu_read_unlock();
//	
//			if (op_is_insert == -1) {
//				if (curr == NULL) op_is_insert = 1;
//				else              op_is_insert = 0;
//			}
//	
//			if (op_is_insert && curr != NULL) return 0;
//			else if (!op_is_insert && curr == NULL) return 2;
//	
//			if (op_is_insert && _do_bst_insert(key, value, prev, curr, direction) == 1)
//				return 1;
//			else if (!op_is_insert && _do_bst_delete(prev, curr, direction) == 1)
//				return 3;
//		}
//	}

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
		if (left && left->key >= root->key)   bst_violations++;
		if (right && right->key <= root->key) bst_violations++;
	
		/* We found a path (a node with at least one NULL child). */
		if (!left || !right) {
			total_paths++;
			if (_th <= min_path_len) min_path_len = _th;
			if (_th >= max_path_len) max_path_len = _th;
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
	
		validate_rec(root->left->left, 0);
	
		check_bst = (bst_violations == 0);
	
		printf("Validation:\n");
		printf("=======================\n");
		printf("  BST Violation: %s\n",
		       check_bst ? "No [OK]" : "Yes [ERROR]");
		printf("  Tree size: %8d\n", total_nodes);
		printf("  Total paths: %d\n", total_paths);
		printf("  Min/max paths length: %d/%d\n", min_path_len, max_path_len);
		printf("\n");
	
		return check_bst;
	}

	/*********************    FOR DEBUGGING ONLY    *******************************/
	void print_rec(node_t *root, int level)
	{
		if (root) print_rec(root->right, level + 1);
	
		for (int i = 0; i < level; i++)
			printf("|--");
	
		if (!root) {
			printf("NULL\n");
			return;
		}
	
		std::cout << root->key << "\n";
	
		print_rec(root->left, level + 1);
	}
	
	void print_struct()
	{
		if (root == NULL) printf("[empty]");
		else              print_rec(root, 0);
		printf("\n");
	}
	/******************************************************************************/
};

#define BST_CITRUS_TEMPL template<typename K, typename V>
#define BST_CITRUS_FUNCT bst_unb_citrus<K,V>

BST_CITRUS_TEMPL
bool BST_CITRUS_FUNCT::contains(const int tid, const K& key)
{
	const V ret = lookup_helper(key);
	return ret != this->NO_VALUE;
}

BST_CITRUS_TEMPL
const std::pair<V,bool> BST_CITRUS_FUNCT::find(const int tid, const K& key)
{
	const V ret = lookup_helper(key);
	return std::pair<V,bool>(ret, ret != this->NO_VALUE);
}

BST_CITRUS_TEMPL
int BST_CITRUS_FUNCT::rangeQuery(const int tid, const K& lo, const K& hi,
                      std::vector<std::pair<K,V>> kv_pairs)
{
	return 0;
}

BST_CITRUS_TEMPL
const V BST_CITRUS_FUNCT::insert(const int tid, const K& key, const V& val)
{
	return insertIfAbsent(tid, key, val);
}

BST_CITRUS_TEMPL
const V BST_CITRUS_FUNCT::insertIfAbsent(const int tid, const K& key, const V& val)
{
	return insert_helper(key, val);
}

BST_CITRUS_TEMPL
const std::pair<V,bool> BST_CITRUS_FUNCT::remove(const int tid, const K& key)
{
	const V ret = delete_helper(key);
	return std::pair<V,bool>(ret, ret != this->NO_VALUE);
}

BST_CITRUS_TEMPL
bool BST_CITRUS_FUNCT::validate()
{
	return validate_helper();
}
