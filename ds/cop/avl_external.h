/**
 * An external COP-based (Consistency-Oblivious Programming) AVL tree.
 **/
#pragma once

#include "../map_if.h"
#include "Log.h"
#include "../locks/lock.h"
#include "../rcu-htm/ht.h"

#define MAX(a,b) ( (a) >= (b) ? (a) : (b) )
#define MAX_HEIGHT 50

#define IS_EXTERNAL_NODE(node) \
    ( (node)->left == NULL && (node)->right == NULL )

template <typename K, typename V>
class avl_ext_cop : public Map<K,V> {
public:
	avl_ext_cop(const K _NO_KEY, const V _NO_VALUE, const int numProcesses)
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
	char *name() { return "AVL External COP"; }

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
		if (!n)	return 0;
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

	/**
	 * Traverses the tree `avl` as dictated by `key`.
	 * When returning, 
	 * - `leaf` is either NULL (empty tree) or points to the external
	 *    node where the traversal ended. 
	 **/
	void traverse(const K& key, node_t **leaf)
	{
		node_t *parent = NULL;
		*leaf = root;
		while (*leaf && !IS_EXTERNAL_NODE(*leaf)) {
			parent = *leaf;
			*leaf = (key <= (*leaf)->key) ? (*leaf)->left : (*leaf)->right;
		}
	}
	
	void lookup_verify(const K& key, node_t *leaf)
	{
		if (!leaf) {
			if (!root)
				return;
			else
				TX_ABORT(ABORT_VALIDATION_FAILURE);
		}
	
		if (leaf->key == key)
			return;
	
		if (!leaf->live)
			TX_ABORT(ABORT_VALIDATION_FAILURE);
	
		node_t *prev = leaf->prev;
		node_t *succ = leaf->succ;
		if (prev && key <= prev->key)
			TX_ABORT(ABORT_VALIDATION_FAILURE);
		if (succ && key >= succ->key)
			TX_ABORT(ABORT_VALIDATION_FAILURE);
	}
	
	void insert_verify(const K& key, node_t *leaf)
	{
		lookup_verify(key, leaf);
	
		if (key == leaf->key) return;
	
		node_t *parent = leaf->parent;
		if (!parent) {
			if (root != leaf)
				TX_ABORT(ABORT_VALIDATION_FAILURE);
		} else {
			if (!parent->live)
				TX_ABORT(ABORT_VALIDATION_FAILURE);
			if (key <= parent->key && parent->left != leaf)
				TX_ABORT(ABORT_VALIDATION_FAILURE);
			if (key > parent->key && parent->right != leaf)
				TX_ABORT(ABORT_VALIDATION_FAILURE);
	
	//		int new_internal_key = (key < leaf->key) ? key : leaf->key;
			int new_internal_key = key;
	//		if (new_internal_key <= parent->key) {
				node_t *prev = parent->prev;
				if (prev && new_internal_key <= prev->key)
					TX_ABORT(ABORT_VALIDATION_FAILURE);
	//		} else {
				node_t *succ = parent->succ;
				if (succ && new_internal_key >= succ->key)
					TX_ABORT(ABORT_VALIDATION_FAILURE);
	//		}
		}
	
		node_t *gparent = parent->parent;
		if (!gparent) {
			if (root != parent)
				TX_ABORT(ABORT_VALIDATION_FAILURE);
		} else {
			if (!gparent->live)
				TX_ABORT(ABORT_VALIDATION_FAILURE);
			if (key <= gparent->key && gparent->left != parent)
				TX_ABORT(ABORT_VALIDATION_FAILURE);
			if (key > gparent->key && gparent->right != parent)
				TX_ABORT(ABORT_VALIDATION_FAILURE);
		}
	
	}
	
	void delete_verify(const K& key, node_t *leaf)
	{
		node_t *parent, *sibling;
	
		insert_verify(key, leaf);
	
		parent = leaf->parent;
		sibling = (key <= parent->key) ? parent->right : parent->left;
		if (sibling && !sibling->live)
			TX_ABORT(ABORT_VALIDATION_FAILURE);
	}

	int lookup_helper(const K& key)
	{
		node_t *leaf;
		tm_begin_ret_t status;
		int ret, retries = -1;
	
	try_from_scratch:
	
		/* Global lock fallback. */
		if (++retries >= TX_NUM_RETRIES) {
			tdata->lacqs++;
			LOCK(&lock);
			traverse(key, &leaf);
			ret = ((leaf != NULL) && (leaf->key == key));
			UNLOCK(&lock);
			return ret;
		}
	
		ret = 0;
	
		/* Asynchronized traversal. */
		traverse(key, &leaf);
	
		/* Transactional verification. */
		while (lock != LOCK_FREE)
			;
	
		tdata->tx_starts++;
		status = TX_BEGIN(0);
		if (status == TM_BEGIN_SUCCESS) {
			if (lock != LOCK_FREE)
				TX_ABORT(ABORT_GL_TAKEN);
	
			/* lookup_verify() will abort if verification fails. */
			lookup_verify(key, leaf);
			ret = ((leaf != NULL) && (leaf->key == key));
	
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
					if (!parent)                 root = rotate_right(curr);
					else if (key <= parent->key) parent->left = rotate_right(curr);
					else                         parent->right = rotate_right(curr);
				} else if (balance2 == -1) { // LEFT-RIGHT case
					curr->left = rotate_left(curr->left);
					if (!parent)                 root = rotate_right(curr); 
					else if (key <= parent->key) parent->left = rotate_right(curr);
					else                         parent->right = rotate_right(curr);
				} else {
					assert(0);
				}
	
				break;
			} else if (balance == -2) {
				int balance2 = node_balance(curr->right);
	
				if (balance2 == -1) { // RIGHT-RIGHT case
					if (!parent)                 root = rotate_left(curr);
					else if (key <= parent->key) parent->left = rotate_left(curr);
					else                         parent->right = rotate_left(curr);
				} else if (balance2 == 1) { // RIGHT-LEFT case
					curr->right = rotate_right(curr->right);
					if (!parent)                 root = rotate_left(curr);
					else if (key <= parent->key) parent->left = rotate_left(curr);
					else                         parent->right = rotate_left(curr);
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
	
	int do_insert(node_t *new_nodes[2], node_t *leaf)
	{
		node_t *prev = NULL, *succ = NULL;
		int key = new_nodes[0]->key;
	
		if (!leaf) {
			new_nodes[1]->live = 1;
			new_nodes[1]->prev = new_nodes[1]->succ = NULL;
			new_nodes[1]->parent = NULL;
			root = new_nodes[1];
			return 1;
		}
	
		node_t *parent = leaf->parent;
	
		if (leaf->key == key) return 0;
	
		//> Create new internal node and its new external child
		node_t *new_internal = new_nodes[0];
		new_internal->live = 1;
		new_internal->prev = new_internal->succ = NULL;
		new_internal->parent = parent;
		if (parent) {
			if (key <= parent->key) {
				new_internal->prev = parent->prev;
				if (parent->prev) parent->prev->succ = new_internal;
				parent->prev = new_internal;
				new_internal->succ = parent;
			} else {
				new_internal->succ = parent->succ;
				if (parent->succ) parent->succ->prev = new_internal;
				parent->succ = new_internal;
				new_internal->prev = parent;
			}
		}
		if (key <= leaf->key) {
			new_internal->left = new_nodes[1];
			new_internal->left->live = 1;
			new_internal->left->prev = leaf->prev;
			new_internal->left->succ = leaf;
			new_internal->left->parent = new_internal;
			new_internal->right = leaf;
			if (leaf->prev) leaf->prev->succ = new_internal->left;
			leaf->prev = new_internal->left;
		} else {
			new_internal->right = new_nodes[1];
			new_internal->right->live = 1;
			new_internal->right->prev = leaf;
			new_internal->right->succ = leaf->succ;
			new_internal->right->parent = new_internal;
			new_internal->key = leaf->key;
			new_internal->left = leaf;
			if (leaf->succ) leaf->succ->prev = new_internal->right;
			leaf->succ = new_internal->right;
		}
		leaf->parent = new_internal;
	
		//> Place the new internal node underneath parent
		if (!parent)                 root = new_internal;
		else if (key <= parent->key) parent->left = new_internal;
		else                         parent->right = new_internal;
	
		insert_fixup(key, new_internal);
		return 1;
	}
	
	int insert_helper(const K& key, const V& value)
	{
		node_t *leaf;
		tm_begin_ret_t status;
		int retries = -1, ret = 0;
	
		node_t *new_nodes[2];
		new_nodes[0] = new node_t(key, NULL);  // Internal
		new_nodes[1] = new node_t(key, value); // External
	
	try_from_scratch:
	
		/* Global lock fallback. */
		if (++retries >= TX_NUM_RETRIES) {
			tdata->lacqs++;
			LOCK(&lock);
			traverse(key, &leaf);
			ret = do_insert(new_nodes, leaf);
			UNLOCK(&lock);
	//		if (!ret)
	//			free(new_node);
			return ret;
		}
	
		/* Asynchronized traversal. */
		traverse(key, &leaf);
	
		/* Transactional verification. */
		while (lock != LOCK_FREE)
			;
	
		tdata->tx_starts++;
		status = TX_BEGIN(0);
		if (status == TM_BEGIN_SUCCESS) {
			if (lock != LOCK_FREE)
				TX_ABORT(ABORT_GL_TAKEN); 
	
			/* _insert_verify() will abort if verification fails. */
			insert_verify(key, leaf);
			ret = do_insert(new_nodes, leaf);
	
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
					if (!parent)                 root = rotate_right(curr);
					else if (key <= parent->key) parent->left = rotate_right(curr);
					else                         parent->right = rotate_right(curr);
				} else if (balance2 == -1) { // LEFT-RIGHT case
					curr->left = rotate_left(curr->left);
					if (!parent)                 root = rotate_right(curr);
					else if (key <= parent->key) parent->left = rotate_right(curr);
					else                         parent->right = rotate_right(curr);
				} else {
					assert(0);
				}
	
				curr = parent;
				continue;
			} else if (balance == -2) {
				int balance2 = node_balance(curr->right);
	
				if (balance2 == 0 || balance2 == -1) { // RIGHT-RIGHT case
					if (!parent)                 root = rotate_left(curr);
					else if (key <= parent->key) parent->left = rotate_left(curr);
					else                         parent->right = rotate_left(curr);
				} else if (balance2 == 1) { // RIGHT-LEFT case
					curr->right = rotate_right(curr->right);
					if (!parent)                 root = rotate_left(curr);
					else if (key <= parent->key) parent->left = rotate_left(curr);
					else                         parent->right = rotate_left(curr);
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
	
	int do_delete(const K& key, node_t *leaf)
	{
		//> Empty tree case
		if (!leaf) return 0;
	
		//> Key not in the tree
		if (leaf->key != key) return 0;
	
		//> Single node tree case
		node_t *parent = leaf->parent;
		if (!parent) {
			leaf->live = 0;
			root = NULL;
			return 1;
		}
	
		//> Fix prev and succ pointers of `parent` siblings
		if (parent->prev) parent->prev->succ = parent->succ;
		if (parent->succ) parent->succ->prev = parent->prev;
	
		//> Normal case with more nodes in the tree
		node_t *sibling;
		if (key <= parent->key) {
			leaf->succ->prev = leaf->prev;
			if (leaf->prev) leaf->prev->succ = leaf->succ;
			sibling = parent->right;
		} else {
			leaf->prev->succ = leaf->succ;
			if (leaf->succ) leaf->succ->prev = leaf->prev;
			sibling = parent->left;
		}
	
		node_t *gparent = parent->parent;
		sibling->parent = gparent;
		if (!gparent)                 root = sibling;
		else if (key <= gparent->key) gparent->left = sibling;
		else                          gparent->right = sibling;
		
		leaf->live = 0;
		parent->live = 0;
	
		delete_fixup(key, gparent);
	
		return 1;
	}
	
	int delete_helper(const K& key)
	{
		node_t *leaf;
		tm_begin_ret_t status;
		int retries = -1, ret = 0;
	
	try_from_scratch:
	
		/* Global lock fallback. */
		if (++retries >= TX_NUM_RETRIES) {
			tdata->lacqs++;
			LOCK(&lock);
			traverse(key, &leaf);
			ret = do_delete(key, leaf);
			UNLOCK(&lock);
			return ret;
		}
	
		/* Asynchronized traversal. */
		traverse(key, &leaf);
	
		/* Transactional verification. */
		while (lock != LOCK_FREE)
			;
	
		tdata->tx_starts++;
		status = TX_BEGIN(0);
		if (status == TM_BEGIN_SUCCESS) {
			if (lock != LOCK_FREE)
				TX_ABORT(ABORT_GL_TAKEN);
	
			/* _delete_verify() will abort if verification fails. */
			delete_verify(key, leaf);
			ret = do_delete(key, leaf);
	
			TX_END(0);
		} else {
			if ((status & _XABORT_EXPLICIT) && (_XABORT_CODE(status) == ABORT_VALIDATION_FAILURE))
				tdata->tx_aborts_explicit_validation++;
			tdata->tx_aborts++;
			goto try_from_scratch;
		}
	
		return ret;
	}

	int total_paths, total_nodes, bst_violations, avl_violations;
	int min_path_len, max_path_len;
	void validate_rec(node_t *root, int _th)
	{
		if (!root) return;
	
		node_t *left = root->left;
		node_t *right = root->right;
	
		total_nodes++;
		_th++;
	
		/* BST violation? */
		if (left && left->key > root->key)    bst_violations++;
		if (right && right->key <= root->key) bst_violations++;
	
		/* AVL violation? */
		int balance = node_balance(root);
		if (balance < -1 || balance > 1) avl_violations++;
	
		/* We found a path */
		if (IS_EXTERNAL_NODE(root)) {
			total_paths++;
			if (_th <= min_path_len) min_path_len = _th;
			if (_th >= max_path_len) max_path_len = _th;
			return;
		}
	
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

#define BST_AVL_EXT_COP_TEMPL template<typename K, typename V>
#define BST_AVL_EXT_COP_FUNCT avl_ext_cop<K,V>

BST_AVL_EXT_COP_TEMPL
bool BST_AVL_EXT_COP_FUNCT::contains(const int tid, const K& key)
{
	return lookup_helper(key);
	return false;
}

BST_AVL_EXT_COP_TEMPL
const std::pair<V,bool> BST_AVL_EXT_COP_FUNCT::find(const int tid, const K& key)
{
	int ret = lookup_helper(key);
	return std::pair<V,bool>(NULL, ret);
	return std::pair<V,bool>(NULL, false);
}

BST_AVL_EXT_COP_TEMPL
int BST_AVL_EXT_COP_FUNCT::rangeQuery(const int tid, const K& lo, const K& hi,
                      std::vector<std::pair<K,V>> kv_pairs)
{
	return 0;
}

BST_AVL_EXT_COP_TEMPL
const V BST_AVL_EXT_COP_FUNCT::insert(const int tid, const K& key, const V& val)
{
	return insertIfAbsent(tid, key, val);
}

BST_AVL_EXT_COP_TEMPL
const V BST_AVL_EXT_COP_FUNCT::insertIfAbsent(const int tid, const K& key, const V& val)
{
	int ret = insert_helper(key, val);
	if (ret == 1) return NULL;
	else return (void *)1;
}

BST_AVL_EXT_COP_TEMPL
const std::pair<V,bool> BST_AVL_EXT_COP_FUNCT::remove(const int tid, const K& key)
{
	int ret = delete_helper(key);
	if (ret == 0) return std::pair<V,bool>(NULL, false);
	else return std::pair<V,bool>((void*)1, true);
}

BST_AVL_EXT_COP_TEMPL
bool BST_AVL_EXT_COP_FUNCT::validate()
{
	return validate_helper();
}
