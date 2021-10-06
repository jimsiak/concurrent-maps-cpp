/**
 * An external unbalanced binary search tree.
 * Paper:
 *    Fast concurrent lock-free binary search trees, Natarajan et. al, PPoPP 2014
 **/

#pragma once

#include "../map_if.h"
#include "Log.h"

#define CAS_PTR(a,b,c) __sync_val_compare_and_swap(a,b,c)

#define MAX_KEY_NATARAJAN 99999999LLU
#define INF2 (MAX_KEY_NATARAJAN)
#define INF1 (MAX_KEY_NATARAJAN - 1)
#define INF0 (MAX_KEY_NATARAJAN - 2)

#define MAX(a,b) ( (a) >= (b) ? (a) : (b) )

//> Helper functions
#define GETFLAG(ptr) ((uint64_t)(ptr) & 1)
#define GETTAG(ptr)  ((uint64_t)(ptr) & 2)
#define FLAG(ptr)    ((node_t *)((uint64_t)(ptr) | 1))
#define TAG(ptr)     ((node_t *)((uint64_t)(ptr) | 2))
#define UNTAG(ptr)   ((node_t *)((uint64_t)(ptr) & 0xfffffffffffffffd))
#define UNFLAG(ptr)  ((node_t *)((uint64_t)(ptr) & 0xfffffffffffffffe))
#define ADDRESS(ptr) ((node_t *)((uint64_t)(ptr) & 0xfffffffffffffffc))

static __thread void* seek_record_threadlocal; //FIXME is this OK to be here??

template <typename K, typename V>
class bst_unb_natarajan: public Map<K,V> {
public:
	bst_unb_natarajan(const K _NO_KEY, const V _NO_VALUE, const int numProcesses)
	  : Map<K,V>(_NO_KEY, _NO_VALUE)
	{
		node_t *r, *s, *inf0, *inf1, *inf2;
		r = new node_t(INF2, NULL);
		s = new node_t(INF1, NULL);
		inf0 = new node_t(INF0, NULL);
		inf1 = new node_t(INF1, NULL);
		inf2 = new node_t(INF2, NULL);
	    asm volatile("" ::: "memory");
	    r->left = s;
	    r->right = inf2;
	    s->right = inf1;
	    s->left= inf0;
	    asm volatile("" ::: "memory");
		root = r;
	}

	void initThread(const int tid) { seek_record_threadlocal = (void*)(new seek_record_t()); };
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
	char *name() { return "BST Unbalanced Natarajan"; }

	void print() { };
	unsigned long long size() { return size_rec(root); };

private:

	struct node_t {
		K key;
		V value;
	
		node_t *left, *right;
		char padding[32];

		node_t(K key, V value) {
			this->key = key;
			this->value = value;
			this->right = this->left = NULL;
		}
	};

	struct seek_record_t {
		node_t *ancestor, *successor,
		       *parent, *leaf;
		char padding[64 - 4 * sizeof(node_t *)];
	};

	node_t *root;

private:

	seek_record_t *seek(const K& key, node_t *node_r) {
		seek_record_t *seek_record = (seek_record_t*)seek_record_threadlocal;
		seek_record_t seek_record_l;
		node_t *node_s = ADDRESS(node_r->left);
		seek_record_l.ancestor = node_r;
		seek_record_l.successor = node_s; 
		seek_record_l.parent = node_s;
		seek_record_l.leaf = ADDRESS(node_s->left);
	
		node_t* parent_field = (node_t*) seek_record_l.parent->left;
		node_t* current_field = (node_t*) seek_record_l.leaf->left;
		node_t* current = ADDRESS(current_field);
	
		while (current != NULL) {
			if (!GETTAG(parent_field)) {
				seek_record_l.ancestor = seek_record_l.parent;
				seek_record_l.successor = seek_record_l.leaf;
			}
			seek_record_l.parent = seek_record_l.leaf;
			seek_record_l.leaf = current;
	
			parent_field = current_field;
			if (key < current->key) current_field = (node_t*) current->left;
			else                    current_field = (node_t*) current->right;
	
			current = ADDRESS(current_field);
		}
		seek_record->ancestor = seek_record_l.ancestor;
		seek_record->successor = seek_record_l.successor;
		seek_record->parent = seek_record_l.parent;
		seek_record->leaf = seek_record_l.leaf;
		return seek_record;
	}
	
	int search(const K& key, node_t *node_r) {
		seek_record_t *seek_record = (seek_record_t*)seek_record_threadlocal;
		seek(key, node_r);
		return (seek_record->leaf->key == key);
	}

	const V lookup_helper(const K& key) {
		seek_record_t *seek_record = (seek_record_t*)seek_record_threadlocal;
		seek(key, root);
		if (seek_record->leaf->key == key) return seek_record->leaf->value;
		else return this->NO_VALUE;
	}

	int cleanup(const K& key) {
		seek_record_t *seek_record = (seek_record_t*)seek_record_threadlocal;
		node_t *ancestor = seek_record->ancestor;
		node_t *successor = seek_record->successor;
		node_t *parent = seek_record->parent;
	
		node_t **succ_addr;
		if (key < ancestor->key) succ_addr = (node_t**) &(ancestor->left);
		else                     succ_addr = (node_t**) &(ancestor->right);
	
		node_t **child_addr;
		node_t **sibling_addr;
		if (key < parent->key) {
			child_addr = (node_t**) &(parent->left);
			sibling_addr = (node_t**) &(parent->right);
		} else {
			child_addr = (node_t**) &(parent->right);
			sibling_addr = (node_t**) &(parent->left);
		}
	
		node_t *chld = *child_addr;
		if (!GETFLAG(chld)) {
			chld = *sibling_addr;
			asm volatile("");
			sibling_addr = child_addr;
		}
	
		while (1) {
			node_t *untagged = *sibling_addr;
			node_t *tagged = TAG(untagged);
			node_t *res = CAS_PTR(sibling_addr, untagged, tagged);
			if (res == untagged) break;
		}
	
		node_t *sibl = *sibling_addr;
		if (CAS_PTR(succ_addr, ADDRESS(successor), UNTAG(sibl)) == ADDRESS(successor))
			return 1;

		return 0;
	}

	bool do_insert(const K& key, const V& val, unsigned *created,
	               node_t **new_internal, node_t **new_node)
	{
		seek_record_t *seek_record = (seek_record_t*)seek_record_threadlocal;
		node_t *parent = seek_record->parent;
		node_t *leaf = seek_record->leaf;
		node_t **child_addr;
	
		if (key < parent->key) child_addr = (node_t**) &(parent->left); 
		else                   child_addr = (node_t**) &(parent->right);
	
		if (*created == 0) {
			*new_internal = new node_t(MAX(key,leaf->key),0);
			*new_node = new node_t(key,val);
			*created = 1;
		} else {
			(*new_internal)->key = MAX(key, leaf->key);
		}
	
		if (key < leaf->key) {
			(*new_internal)->left = *new_node;
			(*new_internal)->right = leaf; 
		} else {
			(*new_internal)->right = *new_node;
			(*new_internal)->left = leaf;
		}
	
		node_t *result = CAS_PTR(child_addr, ADDRESS(leaf), ADDRESS(*new_internal));
		if (result == ADDRESS(leaf))
			return true;
	
		node_t *chld = *child_addr; 
		if ((ADDRESS(chld)==leaf) && (GETFLAG(chld) || GETTAG(chld)))
			cleanup(key);
		return false;
	}

	const V insert_helper(const K& key, const V& val)
	{
		seek_record_t *seek_record = (seek_record_t*)seek_record_threadlocal;
		node_t *new_internal = NULL, *new_node = NULL;
		unsigned created = 0;
		while (1) {
			seek(key, root);
			if (seek_record->leaf->key == key)
	            return seek_record->leaf->value;
			if (do_insert(key, val, &created, &new_internal, &new_node))
				return this->NO_VALUE;
		}
	}

	int do_remove(const K& key, int *injecting, node_t **leaf)
	{
		seek_record_t *seek_record = (seek_record_t*)seek_record_threadlocal;
		node_t *parent = seek_record->parent;
		node_t **child_addr;
		node_t *lf, *result, *chld;
	
		if (key < parent->key) child_addr = (node_t**) &(parent->left);
		else                   child_addr = (node_t**) &(parent->right);
	
		if (*injecting == 1) {
			*leaf = seek_record->leaf;
			if ((*leaf)->key != key)
				return 0;
	
			lf = ADDRESS(*leaf);
			result = CAS_PTR(child_addr, lf, FLAG(lf));
			if (result == ADDRESS(*leaf)) {
				*injecting = 0;
				if (cleanup(key))
					return 1;
			} else {
				chld = *child_addr;
				if ( (ADDRESS(chld) == *leaf) && (GETFLAG(chld) || GETTAG(chld)) )
					cleanup(key);
			}
		} else {
			if (seek_record->leaf != *leaf) {
				return 1; 
			} else {
				if (cleanup(key))
					return 1;
			}
		}
		return -1;
	}
	
	const V delete_helper(const K& key)
	{
		int ret, injecting = 1;
		node_t *leaf;
	
		while (1) {
			seek(key, root);
			ret = do_remove(key, &injecting, &leaf);
			if (ret == 1) return leaf->value;
			else if (ret == 0) return this->NO_VALUE;
	    }
	}

//static int bst_update(skey_t key, sval_t val, node_t* node_r) {
//	int ret, injecting = 1;
//	node_t *leaf;
//	node_t *new_internal = NULL, *new_node = NULL;
//	uint created = 0;
//	int op_is_insert = -1;
//
//	while (1) {
//		bst_seek(key, node_r);
//		if (op_is_insert == -1) {
//			if (seek_record->leaf->key == key) op_is_insert = 0;
//			else                               op_is_insert = 1;
//		}
//
//		if (op_is_insert) {
//			if (seek_record->leaf->key == key)
//				return 0;
//			if (do_bst_insert(key, val, &created, &new_internal, &new_node))
//				return 1;
//		} else {
//			ret = do_bst_remove(key, &injecting, &leaf);
//			if (ret != -1) return ret + 2;
//		}
//    }
//}

	unsigned long long size_rec(node_t* node) {
		if (node == NULL) return 0; 
	
		if ((node->left == NULL) && (node->right == NULL))
			if (node->key < INF0 )
				return 1;
	
		unsigned long long l = 0, r = 0;
		if ( !GETFLAG(node->left) && !GETTAG(node->left))
			l = size_rec(node->left);
		if ( !GETFLAG(node->right) && !GETTAG(node->right))
			r = size_rec(node->right);
		return l+r;
	}

	int total_paths, total_nodes, bst_violations;
	int min_path_len, max_path_len;
	int min_path_key, max_path_key;
	void validate_rec(node_t *root, int _th)
	{
		if (!root) return;
	
		node_t *left = root->left;
		node_t *right = root->right;
	
		if (root->key < INF0) {
			total_nodes++;
			_th++;
		}
	
		/* BST violation? */
		if (left && left->key >= root->key)
			bst_violations++;
		if (right && right->key < root->key)
			bst_violations++;
	
		/* We found a path (a node with at least one sentinel child). */
		if (!left && !right && root->key < INF0) {
			total_paths++;
	
			if (_th <= min_path_len) {
				min_path_len = _th;
				min_path_key = root->key;
			}
			if (_th >= max_path_len) {
				max_path_len = _th;
				max_path_key = root->key;
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
		printf("  Min/max paths length: %d/%d (%d/%d)\n",
		       min_path_len, max_path_len, min_path_key, max_path_key);
		printf("\n");
	
		return check_bst;
	}

};

#define BST_UNB_NATARAJAN_TEMPL template<typename K, typename V>
#define BST_UNB_NATARAJAN_FUNCT bst_unb_natarajan<K,V>

BST_UNB_NATARAJAN_TEMPL
bool BST_UNB_NATARAJAN_FUNCT::contains(const int tid, const K& key)
{
	const V ret = lookup_helper(key);
	return ret != this->NO_VALUE;
}

BST_UNB_NATARAJAN_TEMPL
const std::pair<V,bool> BST_UNB_NATARAJAN_FUNCT::find(const int tid, const K& key)
{
	const V ret = lookup_helper(key);
	return std::pair<V,bool>(ret, ret != this->NO_VALUE);
}

BST_UNB_NATARAJAN_TEMPL
int BST_UNB_NATARAJAN_FUNCT::rangeQuery(const int tid, const K& lo, const K& hi,
                      std::vector<std::pair<K,V>> kv_pairs)
{
	return 0;
}

BST_UNB_NATARAJAN_TEMPL
const V BST_UNB_NATARAJAN_FUNCT::insert(const int tid, const K& key, const V& val)
{
	return insertIfAbsent(tid, key, val);
}

BST_UNB_NATARAJAN_TEMPL
const V BST_UNB_NATARAJAN_FUNCT::insertIfAbsent(const int tid, const K& key, const V& val)
{
	return insert_helper(key, val);
}

BST_UNB_NATARAJAN_TEMPL
const std::pair<V,bool> BST_UNB_NATARAJAN_FUNCT::remove(const int tid, const K& key)
{
	V ret = delete_helper(key);
	return std::pair<V,bool>(ret, (ret != this->NO_VALUE));
}

BST_UNB_NATARAJAN_TEMPL
bool BST_UNB_NATARAJAN_FUNCT::validate()
{
	return validate_helper();
}
