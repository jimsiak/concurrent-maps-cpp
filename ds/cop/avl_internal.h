/**
 * An internal COP-based (Consistency-Oblivious Programming) AVL tree.
 **/
#pragma once

#include "../map_if.h"
#include "Log.h"
#include "../locks/lock.h"
#include "../rcu-htm/ht.h"

#define MAX(a,b) ( (a) >= (b) ? (a) : (b) )
#define MAX_HEIGHT 50

template <typename K, typename V>
class avl_int_cop : public Map<K,V> {
public:
	avl_int_cop(const K _NO_KEY, const V _NO_VALUE, const int numProcesses)
	  : Map<K,V>(_NO_KEY, _NO_VALUE)
	{
		root = NULL;
		INIT_LOCK(&lock);
	}

	void initThread(const int tid) {
		tdata = tdata_new(tid);
	};
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
	char *name() { return "AVL Internal COP"; }

	void print() { print_helper(); };
//	unsigned long long size() { return size_rec(root); };

private:
	const int TX_NUM_RETRIES = 10; //> FIXME

	struct node_t {
		K key;
		V value;
	
		int height;
		int live;
	
		node_t *parent, *left, *right;
		node_t *prev, *succ;
	
		node_t (const K& key, const V& value) {
			this->key = key;
			this->value = value;
			this->height = 1; // new nodes have height 1 and NULL has height 0.
			this->live = 0;
			this->parent = this->left = this->right = NULL;
			this->prev = this->succ = NULL;
		}
	};

	node_t *root;
	node_lock_t lock;
private:

	int node_height(node_t *n)
	{
		if (!n) return 0;
		return n->height;
	}
	
	int node_balance(node_t *n)
	{
		if (!n) return 0;
		return node_height(n->left) - node_height(n->right);
	}
	
	node_t *rotate_right(node_t *node)
	{
		assert(node != NULL && node->left != NULL);
	
		node_t *node_left = node->left;
	
		node->left = node->left->right;
		if (node->left) node->left->parent = node;
	
		node_left->right = node;
		node_left->parent = node->parent;
		node->parent = node_left;
	
		node->height = MAX(node_height(node->left), node_height(node->right)) + 1;
		node_left->height = MAX(node_height(node_left->left), node_height(node_left->right)) + 1;
		return node_left;
	}
	node_t *rotate_left(node_t *node)
	{
		assert(node != NULL && node->right != NULL);
	
		node_t *node_right = node->right;
	
		node->right = node->right->left;
		if (node->right) node->right->parent = node;
	
		node_right->left = node;
		node_right->parent = node->parent;
		node->parent = node_right;
	
		node->height = MAX(node_height(node->left), node_height(node->right)) + 1;
		node_right->height = MAX(node_height(node_right->left), node_height(node_right->right)) + 1;
		return node_right;
	}
	
	/** Returns a pointer to the node that contains `key` or
	 *  a pointer to the node that would be it's parent if `key` was in the tree.
	 **/
	node_t *traverse(const K& key)
	{
		node_t *curr, *prev;
	
		prev = NULL;
		curr = root;
	
		while (curr) {
			if (curr->key == key) return curr;
	
			prev = curr;
			if (key < curr->key) curr = curr->left;
			else                 curr = curr->right;
		}
	
		return prev;
	}
	
	void lookup_verify(node_t *node, const K& key)
	{
		if (!node) {
			if (!root) return;
			else       TX_ABORT(ABORT_VALIDATION_FAILURE);
		}
	
		if (!node->live)
			TX_ABORT(ABORT_VALIDATION_FAILURE);
	
		if (node->key == key)
			return;
	
	//	if (node->key < key) {
			node_t *prev = node->prev;
			if (prev && key <= prev->key)
				TX_ABORT(ABORT_VALIDATION_FAILURE);
	//	} else {
			node_t *succ = node->succ;
			if (succ && key >= succ->key)
				TX_ABORT(ABORT_VALIDATION_FAILURE);
	//	}
	}
	
	void insert_verify(node_t *node, const K& key)
	{
		lookup_verify(node, key);
	
		if (key < node->key && node->left != NULL)
			TX_ABORT(ABORT_VALIDATION_FAILURE);
		else if (key > node->key && node->right != NULL)
			TX_ABORT(ABORT_VALIDATION_FAILURE);
	}
	
	void delete_verify(node_t *node, const K& key)
	{
		lookup_verify(node, key);
	}
	
	int lookup_helper(const K& key)
	{
		node_t *place;
		tm_begin_ret_t status;
		int ret, retries = -1;
	
	try_from_scratch:
	
		/* Global lock fallback. */
		if (++retries >= TX_NUM_RETRIES) {
			tdata->lacqs++;
			LOCK(&lock);
			place = traverse(key);
			ret = (place && place->key == key);
			UNLOCK(&lock);
			return ret;
		}
	
		ret = 0;
	
		/* Asynchronized traversal. */
		place = traverse(key);
	
		/* Transactional verification. */
		while (lock != LOCK_FREE)
			;
	
		tdata->tx_starts++;
		status = TX_BEGIN(0);
		if (status == TM_BEGIN_SUCCESS) {
			if (lock != LOCK_FREE)
				TX_ABORT(ABORT_GL_TAKEN);
	
			/* lookup_verify() will abort if verification fails. */
			lookup_verify(place, key);
			ret = (place && place->key == key);
	
			TX_END(0);
		} else {
			tdata->tx_aborts++;
			goto try_from_scratch;
		}
	
		return ret;
	}
	
	void insert_fixup(const K& key, node_t *place)
	{
		node_t *curr, *parent;
	
		curr = place;
		parent = NULL;
	
		while (curr) {
			parent = curr->parent;
	
			int balance = node_balance(curr);
			if (balance == 2) {
				int balance2 = node_balance(curr->left);
	
				if (balance2 == 1) { // LEFT-LEFT case
					if (!parent)                root = rotate_right(curr);
					else if (key < parent->key) parent->left = rotate_right(curr);
					else                        parent->right = rotate_right(curr);
				} else if (balance2 == -1) { // LEFT-RIGHT case
					curr->left = rotate_left(curr->left);
					if (!parent)                root = rotate_right(curr); 
					else if (key < parent->key) parent->left = rotate_right(curr);
					else                        parent->right = rotate_right(curr);
				} else {
					assert(0);
				}
	
				break;
			} else if (balance == -2) {
				int balance2 = node_balance(curr->right);
	
				if (balance2 == -1) { // RIGHT-RIGHT case
					if (!parent)                root = rotate_left(curr);
					else if (key < parent->key) parent->left = rotate_left(curr);
					else                        parent->right = rotate_left(curr);
				} else if (balance2 == 1) { // RIGHT-LEFT case
					curr->right = rotate_right(curr->right);
					if (!parent)                root = rotate_left(curr);
					else if (key < parent->key) parent->left = rotate_left(curr);
					else                        parent->right = rotate_left(curr);
				} else {
					assert(0);
				}
	
				break;
			}
	
			/* Update the height of current node. */
			int height_saved = node_height(curr);
			int height_new = MAX(node_height(curr->left), node_height(curr->right)) + 1;
			curr->height = height_new;
			if (height_saved == height_new)
				break;
	
			curr = parent;
		}
	}
	
	const V do_insert(node_t *new_node, node_t *place)
	{
		node_t *prev = NULL, *succ = NULL;
	
		if (!place) {
			root = new_node;
			new_node->live = 1;
			return this->NO_VALUE;
		}
	
		if (place->key == new_node->key)
			return place->value;
	
		/* Place the new node in its position. */
		new_node->parent = place;
		new_node->live = 1;
		if (new_node->key < place->key) {
			place->left = new_node;
			succ = place;
			if (succ) prev = succ->prev;
		} else {
			place->right = new_node;
			prev = place;
			if (prev) succ = prev->succ;
		}
	
		new_node->prev = prev;
		new_node->succ = succ;
		if (prev) prev->succ = new_node;
		if (succ) succ->prev = new_node;
	
		insert_fixup(new_node->key, new_node->parent);
		return this->NO_VALUE;
	}
	
	const V insert_helper(const K& key, const V& value)
	{
		node_t *place;
		tm_begin_ret_t status;
		int retries = -1;
		V ret;
	
		node_t *new_node = new node_t(key, value);
	
	try_from_scratch:
	
		/* Global lock fallback. */
		if (++retries >= TX_NUM_RETRIES) {
			tdata->lacqs++;
			LOCK(&lock);
	//		volatile int j; for (j=0; j < 10000000; j++) ; // XXX DEBUG
			place = traverse(key);
			ret = do_insert(new_node, place);
			UNLOCK(&lock);
	//		if (!ret)
	//			free(new_node);
			return ret;
		}
	
		/* Asynchronized traversal. */
		place = traverse(key);
	
		/* Transactional verification. */
		while (lock != LOCK_FREE)
			;
	
		tdata->tx_starts++;
		status = TX_BEGIN(0);
		if (status == TM_BEGIN_SUCCESS) {
			if (lock != LOCK_FREE)
				TX_ABORT(ABORT_GL_TAKEN); 
	
			/* _insert_verify() will abort if verification fails. */
			insert_verify(place, key);
			ret = do_insert(new_node, place);
	
			TX_END(0);
		} else {
			if ((status & _XABORT_EXPLICIT) && (_XABORT_CODE(status) == ABORT_VALIDATION_FAILURE))
				tdata->tx_aborts_explicit_validation++;
			tdata->tx_aborts++;
			goto try_from_scratch;
		}
	
	//	if (!ret)
	//		free(new_node);
	
		return ret;
	}
	
	void delete_fixup(const K& key, node_t *place)
	{
		node_t *curr, *parent;
	
		curr = place;
		parent = NULL;
	
		while (curr) {
			parent = curr->parent;
	
			int balance = node_balance(curr);
			if (balance == 2) {
				int balance2 = node_balance(curr->left);
	
				if (balance2 == 0 || balance2 == 1) { // LEFT-LEFT case
	//				if (!parent)                avl->root = rotate_right(curr);
	//				else if (key < parent->key) parent->left = rotate_right(curr);
					if (!parent)                root = rotate_right(curr);
					else if (key < parent->key) parent->left = rotate_right(curr);
					else                        parent->right = rotate_right(curr);
				} else if (balance2 == -1) { // LEFT-RIGHT case
					curr->left = rotate_left(curr->left);
	//				if (!parent)                avl->root = rotate_right(curr); 
	//				else if (key < parent->key) parent->left = rotate_right(curr);
					if (!parent)                root = rotate_right(curr);
					else if (key < parent->key) parent->left = rotate_right(curr);
					else                        parent->right = rotate_right(curr);
				} else {
					assert(0);
				}
	
				curr = parent;
				continue;
			} else if (balance == -2) {
				int balance2 = node_balance(curr->right);
	
				if (balance2 == 0 || balance2 == -1) { // RIGHT-RIGHT case
	//				if (!parent)                avl->root = rotate_left(curr);
	//				else if (key < parent->key) parent->left = rotate_left(curr);
					if (!parent)                root = rotate_left(curr);
					else if (key < parent->key) parent->left = rotate_left(curr);
					else                        parent->right = rotate_left(curr);
				} else if (balance2 == 1) { // RIGHT-LEFT case
					curr->right = rotate_right(curr->right);
	//				if (!parent)                avl->root = rotate_left(curr);
	//				else if (key < parent->key) parent->left = rotate_left(curr);
					if (!parent)                root = rotate_left(curr);
					else if (key < parent->key) parent->left = rotate_left(curr);
					else                        parent->right = rotate_left(curr);
				} else {
					assert(0);
				}
	
				curr = parent;
				continue;
			}
	
			/* Update the height of current node. */
			int height_saved = node_height(curr);
			int height_new = MAX(node_height(curr->left), node_height(curr->right)) + 1;
			curr->height = height_new;
			if (height_saved == height_new)
				break;
	
			curr = parent;
		}
	}
	
	node_t *minimum_node(node_t *root)
	{
		node_t *ret;
	
		for (ret = root; ret->left != NULL; ret = ret->left)
			;
	
		return ret;
	}
	
	const V do_delete(const K& key, node_t *z)
	{
		node_t *x, *y;
		V del_val;
	
		if (!z || z->key != key) return this->NO_VALUE;
	
		/**
		 * 2 cases for z:
		 *   - zero or one child: z is to be removed.
		 *   - two children: the leftmost node of z's 
		 *     right subtree is to be removed.
		 **/
		if (!z->left || !z->right) y = z;
		else                       y = minimum_node(z->right);
	
		x = y->left;
		if (!x) x = y->right;
	
		/* replace y with x */
		node_t *y_parent = y->parent;
		if (x) x->parent = y_parent;
		if (!y_parent)                root = x;
		else if (y == y_parent->left) y->parent->left = x;
		else                          y->parent->right = x;
	
		del_val = z->value;

		if (y != z) {
			z->key = y->key;
			z->value = y->value;
		}
	
		node_t *prev = y->prev;
		node_t *succ = y->succ;
		if (prev) prev->succ = succ;
		if (succ) succ->prev = prev;
		y->live = 0;

		delete_fixup(y->key, y_parent);
	
		return del_val;
	}
	
	const V delete_helper(const K& key)
	{
		node_t *place;
		tm_begin_ret_t status;
		int retries = -1;
		V ret;
	
	try_from_scratch:
	
		/* Global lock fallback. */
		if (++retries >= TX_NUM_RETRIES) {
			tdata->lacqs++;
			LOCK(&lock);
	//		volatile int j; for (j=0; j < 10000000; j++) ; // XXX DEBUG
			place = traverse(key);
			ret = do_delete(key, place);
			UNLOCK(&lock);
			return ret;
		}
	
		/* Asynchronized traversal. */
		place = traverse(key);
	
		/* Transactional verification. */
		while (lock != LOCK_FREE)
			;
	
		tdata->tx_starts++;
		status = TX_BEGIN(0);
		if (status == TM_BEGIN_SUCCESS) {
			if (lock != LOCK_FREE)
				TX_ABORT(ABORT_GL_TAKEN);
	
			/* _delete_verify() will abort if verification fails. */
			delete_verify(place, key);
			ret = do_delete(key, place);
	
			TX_END(0);
		} else {
			if ((status & _XABORT_EXPLICIT) && (_XABORT_CODE(status) == ABORT_VALIDATION_FAILURE))
				tdata->tx_aborts_explicit_validation++;
			tdata->tx_aborts++;
			goto try_from_scratch;
		}
	
		return ret;
	}
	
//	static int _avl_update_helper(avl_t *avl, int key, void *value, tdata_t *tdata)
//	{
//		avl_node_t *place;
//		tm_begin_ret_t status;
//		int retries = -1, ret = 0;
//		int op_is_insert = -1;
//	
//		avl_node_t *new_node = avl_node_new(key, value);
//	
//	try_from_scratch:
//	
//		/* Global lock fallback. */
//		if (++retries >= TX_NUM_RETRIES) {
//			tdata->lacqs++;
//			pthread_spin_lock(&avl->avl_lock);
//			place = _traverse(avl, key);
//			if (op_is_insert == -1) {
//				if (place->key == key) op_is_insert = 0;
//				else                   op_is_insert = 1;
//			}
//			if (op_is_insert) ret = _insert(avl, new_node, place);
//			else              ret = _delete(avl, key, place);
//			pthread_spin_unlock(&avl->avl_lock);
//			return ret;
//		}
//	
//		/* Asynchronized traversal. */
//		place = _traverse(avl, key);
//		if (op_is_insert == -1) {
//			if (place->key == key) op_is_insert = 0;
//			else                   op_is_insert = 1;
//		}
//	
//		/* Transactional verification. */
//		while (avl->avl_lock != LOCK_FREE)
//			;
//	
//		tdata->tx_starts++;
//		status = TX_BEGIN(0);
//		if (status == TM_BEGIN_SUCCESS) {
//			if (avl->avl_lock != LOCK_FREE)
//				TX_ABORT(ABORT_GL_TAKEN); 
//	
//			if (op_is_insert) {
//				/* _insert_verify() will abort if verification fails. */
//				_insert_verify(avl, place, key);
//				ret = _insert(avl, new_node, place);
//			} else {
//				_delete_verify(avl, place, key);
//				ret = _delete(avl, key, place) + 2;
//			}
//	
//			TX_END(0);
//		} else {
//			if ((status & _XABORT_EXPLICIT) && (_XABORT_CODE(status) == ABORT_VALIDATION_FAILURE))
//				tdata->tx_aborts_explicit_validation++;
//			tdata->tx_aborts++;
//			goto try_from_scratch;
//		}
//	
//		return ret;
//	}
	
	int total_paths, total_nodes, bst_violations, avl_violations;
	int min_path_len, max_path_len;
	void validate_rec(node_t *root, int _th)
	{
		if (!root)
			return;
	
		node_t *left = root->left;
		node_t *right = root->right;
	
		total_nodes++;
		_th++;
	
		/* BST violation? */
		if (left && left->key >= root->key)
			bst_violations++;
		if (right && right->key <= root->key)
			bst_violations++;
	
		if (!left || !right) {
			total_paths++;
	
			if (_th <= min_path_len)
				min_path_len = _th;
			if (_th >= max_path_len)
				max_path_len = _th;
		}
	
		/* AVL violation? */
		int balance = node_balance(root);
		if (balance < -1 || balance > 1)
			avl_violations++;
	
		/* Check subtrees. */
		validate_rec(left, _th);
		validate_rec(right, _th);
	}
	
	int validate_helper()
	{
		int check_bst = 0, check_avl = 0;
		total_paths = 0;
		min_path_len = 99999999;
		max_path_len = -1;
		total_nodes = 0;
		bst_violations = 0;
		avl_violations = 0;
	
		validate_rec(root, 0);
	
		check_bst = (bst_violations == 0);
		check_avl = (avl_violations == 0);
	
		printf("Validation:\n");
		printf("=======================\n");
		printf("  BST Violation: %s\n",
		       check_bst ? "No [OK]" : "Yes [ERROR]");
		printf("  AVL Violation: %s\n",
		       check_avl ? "No [OK]" : "Yes [ERROR]");
		printf("  Tree size: %8d\n", total_nodes);
		printf("  Total paths: %d\n", total_paths);
		printf("  Min/max paths length: %d/%d\n", min_path_len, max_path_len);
		printf("\n");
	
		return check_bst && check_avl;
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
	
		printf("%d (%d, %d)\n", root->key,
		       root->prev ? root->prev->key : -1,
		       root->succ ? root->succ->key : -1);
	
		print_rec(root->left, level + 1);
	}
	
	void print_helper()
	{
		if (!root) printf("[empty]");
		else       print_rec(root, 0);
		printf("\n");
	}
	/******************************************************************************/
};

#define BST_AVL_INT_COP_TEMPL template<typename K, typename V>
#define BST_AVL_INT_COP_FUNCT avl_int_cop<K,V>

BST_AVL_INT_COP_TEMPL
bool BST_AVL_INT_COP_FUNCT::contains(const int tid, const K& key)
{
	return lookup_helper(key);
	return false;
}

BST_AVL_INT_COP_TEMPL
const std::pair<V,bool> BST_AVL_INT_COP_FUNCT::find(const int tid, const K& key)
{
	int ret = lookup_helper(key);
	return std::pair<V,bool>(NULL, ret);
	return std::pair<V,bool>(NULL, false);
}

BST_AVL_INT_COP_TEMPL
int BST_AVL_INT_COP_FUNCT::rangeQuery(const int tid, const K& lo, const K& hi,
                      std::vector<std::pair<K,V>> kv_pairs)
{
	return 0;
}

BST_AVL_INT_COP_TEMPL
const V BST_AVL_INT_COP_FUNCT::insert(const int tid, const K& key, const V& val)
{
	return insertIfAbsent(tid, key, val);
}

BST_AVL_INT_COP_TEMPL
const V BST_AVL_INT_COP_FUNCT::insertIfAbsent(const int tid, const K& key, const V& val)
{
	return insert_helper(key, val);
}

BST_AVL_INT_COP_TEMPL
const std::pair<V,bool> BST_AVL_INT_COP_FUNCT::remove(const int tid, const K& key)
{
	const V ret = delete_helper(key);
	return std::pair<V,bool>(ret, ret != this->NO_VALUE);
}

BST_AVL_INT_COP_TEMPL
bool BST_AVL_INT_COP_FUNCT::validate()
{
	return validate_helper();
}
