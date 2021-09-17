/**
 * A partially external relaxed-balanced AVL tree.
 * Paper:
 * The Contention-Friendly Tree,  Crain et. al, EuroPar 2013
 * See also here for the equivalent java code: 
 *    https://github.com/gramoli/synchrobench/blob/master/java/src/trees/lockbased/LockBasedFriendlyTreeMap.java
 **/
#pragma once

#include <pthread.h>
#include <unistd.h> //> For usleep()

#include "../map_if.h"
#include "Log.h"
#include "lock.h"

template <typename K, typename V>
class bst_avl_cf : public Map<K,V> {
public:
	bst_avl_cf(const K _NO_KEY, const V _NO_VALUE, const int numProcesses)
	{
		root = new node_t(0, NULL);
		spawn_maintenance_thread();
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
	char *name() { return "BST AVL Contention-Friendly"; }

	void print() { print_struct(); };
//	unsigned long long size() { return size_rec(root); };

private:

	struct node_t {
		K key;
		V value;

		node_t *left, *right;
		node_lock_t lock;

		#define REM_BY_LEFT_ROT 10
		char del, // node logically removed
		     rem; // node physically removed

		int left_h,
		    right_h,
		    local_h;

		node_t(const K& key, const V& value) {
			this->key = key;
			this->value = value;
			this->left = this->right = NULL;
			INIT_LOCK(&lock);
			this->del = this->rem = false;
			this->left_h = this->right_h = this->local_h = 0;
		};
	};

	node_t *root;

private:
	/*****************************************************************************/
	/*          Beginning of asynchronous balancing functions                    */
	/*****************************************************************************/
	int remove_node(node_t *parent, int left_child)
	{
		node_t *n, *child;
	
		if (parent->rem) return 0;
		if (left_child) n = parent->left;
		else            n = parent->right;
		if (!n) return 0;
	
		LOCK(&parent->lock);
		LOCK(&n->lock);
		if (!n->del) {
			UNLOCK(&n->lock);
			UNLOCK(&parent->lock);
			return 0;
		}
	
		if ((child = n->left) != NULL) {
			if (n->right != NULL) {
				UNLOCK(&n->lock);
				UNLOCK(&parent->lock);
				return 0;
			}
		} else {
			child = n->right;
		}
	
		if (left_child) parent->left = child;
		else            parent->right = child;
	
		n->left = parent;
		n->right = parent;
		n->rem = 1;
		UNLOCK(&n->lock);
		UNLOCK(&parent->lock);
	
		//>	update_node_heights();
		if (left_child) parent->left_h = n->local_h - 1;
		else            parent->right_h = n->local_h - 1;
		parent->local_h = MAX(parent->left_h, parent->right_h) + 1;
	
		return 1;
	}
	
	void propagate(node_t *n)
	{
		node_t *left = n->left;
		node_t *right = n->right;
	
		if (left)  n->left_h = left->local_h;
		else       n->left_h = 0; 
		if (right) n->right_h = right->local_h;
		else       n->right_h = 0; 
		n->local_h = MAX(n->left_h, n->right_h) + 1;
	}
	
	void rotate_right(node_t *parent, int left_child)
	{
		node_t *n, *l, *r, *lr;
		node_t *new_node;
	
		if (parent->rem) return;
		if (left_child) n = parent->left;
		else            n = parent->right;
		if (!n) return;
	
		l = n->left;
		if (!l) return;
	
		LOCK(&parent->lock);
		LOCK(&n->lock);
		LOCK(&l->lock);
	
		lr = l->right;
		r = n->right;
	
		new_node = new node_t(n->key, n->value);
		new_node->del = n->del;
		new_node->rem = n->rem;
		new_node->left = lr;
		new_node->right = r;
		l->right = new_node;
	
		n->rem = 1;
	
		if (left_child) parent->left = l;
		else            parent->right = l;
	
		UNLOCK(&l->lock);
		UNLOCK(&n->lock);
		UNLOCK(&parent->lock);
	
		//> update_node_heights();
		propagate(new_node);
		l->right_h = new_node->local_h;
		l->local_h = MAX(l->left_h, l->right_h) + 1;
		if (left_child) parent->left_h = l->local_h;
		else            parent->right_h = l->local_h;
		parent->local_h = MAX(parent->left_h, parent->right_h) + 1;
		return;
	}
	
	void rotate_left(node_t *parent, int left_child)
	{
		node_t *n, *l, *r, *rl;
		node_t *new_node;
	
		if (parent->rem) return;
		if (left_child) n = parent->left;
		else            n = parent->right;
		if (!n) return;
	
		r = n->right;
		if (!r) return;
	
		LOCK(&parent->lock);
		LOCK(&n->lock);
		LOCK(&r->lock);
	
		rl = r->left;
		l = n->left;
	
		new_node = new node_t(n->key, n->value);
		new_node->del = n->del;
		new_node->rem = n->rem;
		new_node->left = l;
		new_node->right = rl;
		r->left = new_node;
	
		n->rem = REM_BY_LEFT_ROT;
	
		if (left_child) parent->left = r;
		else            parent->right = r;
	
		UNLOCK(&r->lock);
		UNLOCK(&n->lock);
		UNLOCK(&parent->lock);
	
		//> update_node_heights();
		propagate(new_node);
		r->left_h = new_node->local_h;
		r->local_h = MAX(r->left_h, r->right_h) + 1;
		if (left_child) parent->left_h = r->local_h;
		else            parent->right_h = r->local_h;
		parent->local_h = MAX(parent->left_h, parent->right_h) + 1;
		return;
	}
	
	void rebalance_node(node_t *parent, node_t *node, int left_child)
	{
		int balance, balance2;
		node_t *left, *right;
	
		balance = node->left_h - node->right_h;
		if (balance >= 2) {
			left = node->left;
			balance2 = left->left_h - left->right_h;
			if (balance2 >= 0) {         // LEFT-LEFT case
				rotate_right(parent, left_child);
			} else if (balance2 < 0) { // LEFT-RIGHT case
				rotate_left(node, 1);
				rotate_right(parent, left_child);
			}
		} else if (balance <= -2) {
			right = node->right;
			balance2 = right->left_h - right->right_h;
			if (balance2 < 0) {         // RIGHT-RIGHT case
				rotate_left(parent, left_child);
			} else if (balance2 >= 0) { // RIGHT-LEFT case
				rotate_right(node, 0);
				rotate_left(parent, left_child);
			}
		}
	}
	
	void restructure_node(node_t *parent, node_t *node, int left_child)
	{
		if (!node) return;
	
		node_t *left = node->left;
		node_t *right = node->right;
	
		//> Remove node if needed
		if (!node->rem && node->del && (!left || !right) && node != root)
			if (remove_node(parent, left_child))
				return;
	
		//> Restructure subtrees
		if (!node->rem) {
			restructure_node(node, left, 1);
			restructure_node(node, right, 0);
		}
	
		if (!node->rem && node != root) {
			propagate(node);
			rebalance_node(parent, node, left_child);
		}
	}
	
	volatile int stop_maint_thread;
	pthread_t maint_thread;
	static void *background_struct_adaptation(void *arg)
	{
		bst_avl_cf *tree = (bst_avl_cf *)arg;
		volatile int i = 0;
	
		//> Initialize appropriately the node_allocation struct.
//		node_allocator_init_thread(-2);
	
		while (!tree->stop_maint_thread) {
			tree->restructure_node(tree->root, tree->root->right, 0);
	//		usleep(200000);
			usleep(2000);
		}
	
		//> Do some more restructure before exiting
	//	for (i=0; i < 10; i++)
	//		restructure_node(avl, avl->root, avl->root->right, 0);
		return NULL;
	}
	
	void spawn_maintenance_thread()
	{
		stop_maint_thread = 0;
		pthread_create(&maint_thread, NULL, background_struct_adaptation, this);
	}
	void stop_maintenance_thread()
	{
		stop_maint_thread = 1;
		pthread_join(maint_thread, NULL);
	}
	/*****************************************************************************/
	/*****************************************************************************/
	/*****************************************************************************/

	node_t *get_next(node_t *node, const K& key)
	{
		node_t *next;
		char rem = node->rem;
	
		if (rem == REM_BY_LEFT_ROT) next = node->right;
		else if (rem)               next = node->left;
		else if (key < node->key)   next = node->left;
		else if (node->key == key)  next = NULL;
		else                        next = node->right;
		return next;
	}
	
	void traverse(const K& key, node_t **parent, node_t **leaf)
	{
		*parent = NULL;
		*leaf = root;
	
		while (*leaf) {
			if ((*leaf)->key == key)
				return;
	
			*parent = *leaf;
			*leaf = (key < (*leaf)->key) ? (*leaf)->left : (*leaf)->right;
		}
	}
	
	int lookup_helper(const K& key)
	{
		node_t *parent, *leaf;
		traverse(key, &parent, &leaf);
		return (leaf != NULL && !leaf->del);
	}
	
	int validate(node_t *node, const K& key)
	{
		node_t *next;
		if (node->rem)             return 0;
		else if (key == node->key) return 1;
		else if (key < node->key) next = node->left;
		else                      next = node->right;
	
		if (!next) return 1;
		else       return 0;
	}
	
	int do_traverse(const K& key, node_t **curr, node_t **next)
	{
	start:
		*curr = root;
		while (1) {
			*next = get_next(*curr, key);
			if (!(*next)) {
				LOCK(&(*curr)->lock);
				if (validate(*curr, key)) break;
				UNLOCK(&(*curr)->lock);
				goto start;
			} else {
				*curr = *next;
			}
		}
		return 1;
	}
	
	int do_insert(const K& key, const V& value, node_t *curr)
	{
		int ret = 0;
		if (key == curr->key) {
			if (curr->del) {
				curr->del = 0;
				ret = 1;
			}
		} else {
			if (key < curr->key) curr->left  = new node_t(key, value);
			else                 curr->right = new node_t(key, value);
			ret = 1;
		}
		UNLOCK(&curr->lock);
		return ret;
	}
	
	int insert_helper(const K& key, const V& value)
	{
		node_t *curr, *next;
		int ret = 0;
		ret = do_traverse(key, &curr, &next);
		if (ret == 1) ret = do_insert(key, value, curr);
		return ret;
	}
	
	int do_delete(const K& key, node_t *curr)
	{
		int ret = 0;
		if (key == curr->key && !curr->rem && !curr->del) {
			curr->del = 1;
			ret = 1;
		}
		UNLOCK(&curr->lock);
		return ret;
	}
	
	int delete_helper(const K& key)
	{
		node_t *curr, *next;
		int ret = 0;
		ret = do_traverse(key, &curr, &next);
		if (ret == 1) ret = do_delete(key, curr);
		return ret;
	}

	int update_helper(const K& key, const V& value)
	{
		node_t *curr, *next;
		int ret = 0;
		ret = do_traverse(key, &curr, &next);
		if (curr->key != key || curr->del) ret = do_insert(key, value, curr);
		else                               ret = do_delete(key, curr) + 2;
		return ret;
	}

	int total_paths, total_nodes, marked_nodes;
	int bst_violations, avl_violations;
	int min_path_len, max_path_len;
	void validate_rec(node_t *root, int _th)
	{
		if (!root)
			return;
	
		node_t *left = root->left;
		node_t *right = root->right;
	
		total_nodes++;
		_th++;
		if (root->del) marked_nodes++;
	
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
	
		/* AVL violation? */
		int balance = root->left_h - root->right_h;
		if (balance < -1 || balance > 1)
			avl_violations++;
	
		/* Check subtrees. */
		if (left)  validate_rec(left, _th);
		if (right) validate_rec(right, _th);
	}
	
	int validate_helper()
	{
		int check_bst = 0, check_avl = 0;
		total_paths = 0;
		min_path_len = 99999999;
		max_path_len = -1;
		total_nodes = 0;
		marked_nodes = 0;
		bst_violations = 0;
		avl_violations = 0;
	
		validate_rec(root->right, 0);
	
		check_bst = (bst_violations == 0);
		check_avl = (avl_violations == 0);
	
		printf("Validation:\n");
		printf("=======================\n");
		printf("  BST Violation: %s\n",
		       check_bst ? "No [OK]" : "Yes [ERROR]");
		printf("  AVL Violation: %s\n",
		       check_avl ? "No [OK]" : "Yes [ERROR]");
		printf("  Tree size (Total [Marked / Unmarked]): %8d [%8d / %8d]\n",
		              total_nodes, marked_nodes, total_nodes - marked_nodes);
		printf("  Total paths: %d\n", total_paths);
		printf("  Min/max paths length: %d/%d\n", min_path_len, max_path_len);
		printf("\n");
	
		return check_bst && check_avl;
	}
	
	/*********************    FOR DEBUGGING ONLY    *******************************/
	void print_rec(node_t *root, int level)
	{
		int i;
	
		if (root)
			print_rec(root->right, level + 1);
	
		for (i = 0; i < level; i++)
			std::cout << "|--";
	
		if (!root) {
			std::cout << "NULL\n";
			return;
		}
	
		if (!root->del)
			std::cout << root->key << " [" << root->local_h << "("
			          << root->left_h << "-" << root->right_h << ")]\n";
		else
			std::cout << "{" << root->key << "} [" << root->local_h << "("
			          << root->left_h << "-" << root->right_h << ")]\n";
	
		print_rec(root->left, level + 1);
	}
	
	void print_struct()
	{
		if (root->right == NULL) std::cout << "[empty]";
		else  print_rec(root->right, 0);
		printf("\n");
	}
	/******************************************************************************/
};

#define BST_AVL_CF_TEMPL template<typename K, typename V>
#define BST_AVL_CF_FUNCT bst_avl_cf<K,V>

BST_AVL_CF_TEMPL
bool BST_AVL_CF_FUNCT::contains(const int tid, const K& key)
{
	return lookup_helper(key);
}

BST_AVL_CF_TEMPL
const std::pair<V,bool> BST_AVL_CF_FUNCT::find(const int tid, const K& key)
{
	int ret = lookup_helper(key);
	return std::pair<V,bool>(NULL, ret);
}

BST_AVL_CF_TEMPL
int BST_AVL_CF_FUNCT::rangeQuery(const int tid, const K& lo, const K& hi,
                      std::vector<std::pair<K,V>> kv_pairs)
{
	return 0;
}

BST_AVL_CF_TEMPL
const V BST_AVL_CF_FUNCT::insert(const int tid, const K& key, const V& val)
{
	return insertIfAbsent(tid, key, val);
}

BST_AVL_CF_TEMPL
const V BST_AVL_CF_FUNCT::insertIfAbsent(const int tid, const K& key, const V& val)
{
	int ret = insert_helper(key, val);
	if (ret == 1) return NULL;
	else return (void *)1;
}

BST_AVL_CF_TEMPL
const std::pair<V,bool> BST_AVL_CF_FUNCT::remove(const int tid, const K& key)
{
	int ret = delete_helper(key);
	if (ret == 0) return std::pair<V,bool>(NULL, false);
	else return std::pair<V,bool>((void*)1, true);
}

BST_AVL_CF_TEMPL
bool BST_AVL_CF_FUNCT::validate()
{
	for (int i=0; i < 10; i++) restructure_node(root, root->right, 0);
	stop_maintenance_thread();
	return validate_helper();
}











