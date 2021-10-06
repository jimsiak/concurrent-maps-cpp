/**
 * A partially external relaxed-balanced AVL tree.
 * Paper:
 *    A practical concurrent binary search tree, Bronson et. al, PPoPP 2010
 **/

/**
 * NOTES:
 * The `value` field of `node_t` is used to mark a node, so we have a problem
 * with having V as a value type.
 * Actually using `value` to mark nodes did indeed create a problem.
 * At 17/9/2021 while debugging I encountered the problem when a node with value
 * 0xFFFF (dec: 65535) was falsely regarded as a marked node. To bypass the
 * problem I set MARKED_NODE to 0xffffffff and added an assert statement when
 * inserting a node with `value` equal to MARKED_NODE.
 **/

#pragma once

#include "../map_if.h"
#include "Log.h"
#include "lock.h"

#define MAX(a,b) ( (a) >= (b) ? (a) : (b) )

//> node->version handling
#define UNLINKED 0x1LL
#define SHRINKING 0x2LL
#define SHRINK_CNT_INC (0x1LL << 2)
#define IS_SHRINKING(version) (version & SHRINKING)

#define LEFT 0
#define RIGHT 1
#define GET_CHILD_DIR(node, dir) (((dir) == LEFT) ? (node)->left : (node)->right)
#define GET_CHILD_KEY(node, key) ((key < (node)->key) ? (node)->left : (node)->right)

#define RETRY 0xFF
//> Used as `value` for marked nodes (zombie nodes)
#define MARKED_NODE ((void *)0xffffffffLLU)

//#define SW_BARRIER() __sync_synchronize()
#define SW_BARRIER() asm volatile("": : :"memory")

#define BEGIN_SHRINKING(n) (n)->version |= SHRINKING; SW_BARRIER()
#define END_SHRINKING(n) SW_BARRIER(); (n)->version += SHRINK_CNT_INC; (n)->version &= (~SHRINKING)

template <typename K, typename V>
class bst_avl_bronson : public Map<K,V> {
public:
	bst_avl_bronson(const K _NO_KEY, const V _NO_VALUE, const int numProcesses)
	  : Map<K,V>(_NO_KEY, _NO_VALUE)
	{
		root = new node_t(99999999, 0, 0, 0, NULL);
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
	char *name() { return "BST AVL Bronson"; }

	void print() { print_struct(); };
//	unsigned long long size() { return size_rec(root); };

private:

	struct node_t {
		K key;
		V value;
	
		int height;
		long long version;

		node_lock_t lock;
	
		node_t *left, *right, *parent;
		char padding[32];

		node_t(K key, V value, int height, long long version, node_t *parent) {
			this->key = key;
			this->value = value;
			this->height = height;
			this->version = version;
			this->parent = parent;
			this->right = this->left = NULL;
			INIT_LOCK(&this->lock);
		}

	};

	node_t *root;

private:

	#define SPIN_CNT 100
	void wait_until_not_changing(node_t *n)
	{
		volatile int i = 0;
		long long version = n->version;
	
		if (IS_SHRINKING(version)) {
			while (n->version == version && i < SPIN_CNT) ++i;
			if (i == SPIN_CNT) {
				LOCK(&n->lock);
				UNLOCK(&n->lock);
			}
		}
	}
	
	int attempt_get(const K& key, node_t *node, int dir, long long version,
	                const V& retval)
	{
		node_t *child;
		int next_dir, ret;
		long long child_version;
	
		while (1) {
			child = GET_CHILD_DIR(node, dir);
			SW_BARRIER();
	
			//> The node version has changed. Must retry.
			if (node->version != version) return RETRY;
	
			//> Reached NULL or the node with the specified key.
			if (child == NULL) {
				retval = this->NO_VALUE;
				return 0;
			}
			if (key == child->key) {
				retval = child->value;
				return 1;
			}
	
			//> Where to go next?
			next_dir = (key < child->key) ? LEFT : RIGHT;
	
			child_version = child->version;
			if (IS_SHRINKING(child_version)) {
				wait_until_not_changing(child);
			} else if (child_version != UNLINKED && child == GET_CHILD_DIR(node, dir)) {
				if (node->version != version) return RETRY;
				ret = attempt_get(key, child, next_dir, child_version, retval);
				if (ret != RETRY) return ret;
			}
		}
	}
	
	const V lookup_helper(const K& key)
	{
		int ret;
		const V retval;
		ret = attempt_get(key, root, RIGHT, 0, retval);
		assert(ret != RETRY);
		return retval;
	}

	/*****************************************************************************/
	/* Rebalancing functions                                                     */
	/*****************************************************************************/
	#define UNLINK_REQUIRED -1
	#define REBALANCE_REQUIRED -2
	#define NOTHING_REQUIRED -3
	
	#define NODE_HEIGHT(node) ((node == NULL) ? 0 : node->height)
	
	int node_condition(node_t *node)
	{
		node_t *nl, *nr;
		int hn, hl, hr, hn_new, balance;
	
		nl = node->left;
		nr = node->right;
		if ((!nl || !nr) && node->value == MARKED_NODE) return UNLINK_REQUIRED;
	
		hn = node->height;
		hl = NODE_HEIGHT(nl);
		hr = NODE_HEIGHT(nr);
	
		hn_new = 1 + MAX(hl, hr);
		balance = hl - hr;
		if (balance < -1 || balance > 1) return REBALANCE_REQUIRED;
	
		return (hn != hn_new) ? hn_new : NOTHING_REQUIRED;
	}
	//> `node` must be locked before calling.
	node_t *fix_node_height(node_t *node)
	{
		int c = node_condition(node);
		if (c == REBALANCE_REQUIRED || c == UNLINK_REQUIRED) return node;
		if (c == NOTHING_REQUIRED) return NULL;
		node->height = c;
		return node->parent;
	}
	node_t *rotate_right(node_t *parent, node_t *n, node_t *nl, int hr, int hll,
	                     node_t *nlr, int hlr)
	{
		int hn_new, balance_n, balance_l;
	
		BEGIN_SHRINKING(n);
	
		n->left = nlr;
		if (nlr) nlr->parent = n;
		nl->right = n;
		n->parent = nl;
		if (parent->left == n) parent->left = nl;
		else                  parent->right = nl;
		nl->parent = parent;
	
		hn_new = 1 + MAX(hlr, hr);
		n->height = hn_new;
		nl->height = 1 + MAX(hll, hn_new);
	
		END_SHRINKING(n);
	
		balance_n = hlr - hr;
		if (balance_n < -1 || balance_n > 1) return n;
		if ((!nlr || hr == 0) && n->value == MARKED_NODE) return n;
	
		balance_l = hll - hn_new;
		if (balance_l < -1 || balance_l > 1) return nl;
		if (hll == 0 && nl->value == MARKED_NODE) return nl;
		return fix_node_height(parent);
	}
	node_t *rotate_right_over_left(node_t *parent, node_t *n, node_t *nl, int hr, int hll,
	                               node_t *nlr, int hlrl)
	{
		node_t *nlrl, *nlrr;
		int hlrr, hn_new, hl_new, balance_n, balance_lr;
	
		nlrl = nlr->left;
		nlrr = nlr->right;
		hlrr = NODE_HEIGHT(nlrr);
	
		BEGIN_SHRINKING(n);
		BEGIN_SHRINKING(nl);
		
		n->left = nlrr;
		if (nlrr) nlrr->parent = n;
		nl->right = nlrl;
		if (nlrl) nlrl->parent = nl;
		nlr->left = nl;
		nl->parent = nlr;
		nlr->right = n;
		n->parent = nlr;
		if (parent->left == n) parent->left = nlr;
		else                   parent->right = nlr;
		nlr->parent = parent;
	
		hn_new = 1 + MAX(hlrr, hr);
		n->height = hn_new;
		hl_new = 1 + MAX(hll, hlrl);
		nl->height = hl_new;
		nlr->height = 1 + MAX(hl_new, hn_new);
	
		END_SHRINKING(n);
		END_SHRINKING(nl);
	
		balance_n = hlrr - hr;
		if (balance_n < -1 || balance_n > 1) return n;
//		if ((!nlrr || hr == 0) && n->data == MARKED_NODE) return n;
	
		balance_lr = hl_new - hn_new;
		if (balance_lr < -1 || balance_lr > 1) return nlr;
	
		return fix_node_height(parent);
	}
	node_t *rebalance_right(node_t *parent, node_t *n, node_t *nl, int hr)
	{
		node_t *nlr, *ret;
		int hl, hll, hlr, hlrl, balance;
	
		LOCK(&nl->lock);
	
		hl = nl->height;
		if (hl - hr <= 1) {
			UNLOCK(&nl->lock);
			return n;
		}
	
		nlr = nl->right;
		hll = NODE_HEIGHT(nl->left);
		hlr = NODE_HEIGHT(nlr);
		if (hll >= hlr) {
			ret = rotate_right(parent, n, nl, hr, hll, nlr, hlr);
			UNLOCK(&nl->lock);
			return ret;
		}
	
		LOCK(&nlr->lock);
		hlr = nlr->height;
		if (hll >= hlr) {
			ret = rotate_right(parent, n, nl, hr, hll, nlr, hlr);
			UNLOCK(&nlr->lock);
			UNLOCK(&nl->lock);
			return ret;
		}
	
		hlrl = NODE_HEIGHT(nlr->left);
		balance = hll - hlrl;
//		if (balance >= -1 && balance <= 1 &&
//		    !((hll == 0 || hlrl == 0) && nl->data == MARKED_NODE)) {
//		if (balance >= -1 && balance <= 1 && !(hll == 0 || hlrl == 0)) {
//		if (1) {
			ret = rotate_right_over_left(parent, n, nl, hr, hll, nlr, hlrl);
			UNLOCK(&nlr->lock);
			UNLOCK(&nl->lock);
			return ret;
	//	}
	//	UNLOCK(&nlr->lock);
	//
	//	ret = rebalance_left(n, nl, nlr, hll);
	//	UNLOCK(&nl->lock);
	//	return ret;
	}
	node_t *rotate_left(node_t *parent, node_t *n, int hl,
	                    node_t *nr, node_t *nrl, int hrl, int hrr)
	{
		int hn_new, balance_n, balance_r;
	
		BEGIN_SHRINKING(n);
	
		n->right = nrl;
		if (nrl) nrl->parent = n;
		nr->left = n;
		n->parent = nr;
		if (parent->left == n) parent->left = nr;
		else                   parent->right = nr;
		nr->parent = parent;
	
		hn_new = 1 + MAX(hl, hrl);
		n->height = hn_new;
		nr->height = 1 + MAX(hn_new, hrr);
	
		END_SHRINKING(n);
	
		balance_n = hrl - hl;
		if (balance_n < -1 || balance_n > 1) return n;
		if ((!nrl || hl == 0) && n->value == MARKED_NODE) return n;
	
		balance_r = hrr - hn_new;
		if (balance_r < -1 || balance_r > 1) return nr;
		if (hrr == 0 && nr->value == MARKED_NODE) return nr;
		return fix_node_height(parent);
	
	}
	node_t *rotate_left_over_right(node_t *parent, node_t *n, int hl,
	                               node_t *nr, node_t *nrl,
	                               int hrr, int hrlr)
	{
		node_t *nrll, *nrlr;
		int hrll, hn_new, hr_new, balance_n, balance_rl;
	
		nrll = nrl->left;
		nrlr = nrl->right;
		hrll = NODE_HEIGHT(nrll);
	
		BEGIN_SHRINKING(n);
		BEGIN_SHRINKING(nr);
		
		n->right = nrll;
		if (nrll) nrll->parent = n;
		nr->left = nrlr;
		if (nrlr) nrlr->parent = nr;
		nrl->right = nr;
		nr->parent = nrl;
		nrl->left = n;
		n->parent = nrl;
		if (parent->left == n) parent->left = nrl;
		else                   parent->right = nrl;
		nrl->parent = parent;
	
		hn_new = 1 + MAX(hl, hrll);
		n->height = hn_new;
		hr_new = 1 + MAX(hrlr, hrr);
		nr->height = hr_new;
		nrl->height = 1 + MAX(hn_new, hr_new);
	
		END_SHRINKING(n);
		END_SHRINKING(nr);
	
		balance_n = hrll - hl;
		if (balance_n < -1 || balance_n > 1) return n;
//		if ((!nrll || hl == 0) && n->data == MARKED_NODE) return n;
	
		balance_rl = hr_new - hn_new;
		if (balance_rl < -1 || balance_rl > 1) return nrl;
	
		return fix_node_height(parent);
	}
	node_t *rebalance_left(node_t *parent, node_t *n, node_t *nr, int hl)
	{
		int hr, hrl, hrr, hrlr, balance;
		node_t *nrl, *ret;
	
		LOCK(&nr->lock);
		hr = nr->height;
		if (hl - hr >= -1) {
			UNLOCK(&nr->lock);
			return n;
		}
	
		nrl = nr->left;
		hrl = NODE_HEIGHT(nrl);
		hrr = NODE_HEIGHT(nr->right);
		if (hrr >= hrl) {
			ret = rotate_left(parent, n, hl, nr, nrl, hrl, hrr);
			UNLOCK(&nr->lock);
			return ret;
		}
	
		LOCK(&nrl->lock);
		hrl = nrl->height;
		if (hrr >= hrl) {
			ret = rotate_left(parent, n, hl, nr, nrl, hrl, hrr);
			UNLOCK(&nrl->lock);
			UNLOCK(&nr->lock);
			return ret;
		}
	
		hrlr = NODE_HEIGHT(nrl->right);
		balance = hrr - hrlr;
//		if (balance >= -1 && balance <= 1 &&
//		    !((hrr == 0 || hrlr == 0) && nr->data == MARKED_NODE)) {
//		if (balance >= -1 && balance <= 1 && !(hrr == 0 || hrlr == 0)) {
//		if (1) {
			ret = rotate_left_over_right(parent, n, hl, nr, nrl, hrr, hrlr);
			UNLOCK(&nrl->lock);
			UNLOCK(&nr->lock);
			return ret;
//		}
//		UNLOCK(&nrl->lock);
//
//		ret = rebalance_right(n, nr, nrl, hrr);
//		UNLOCK(&nr->lock);
//		return ret;
	}
	int attempt_node_unlink(node_t *parent, node_t *n)
	{
		node_t *l = n->left,
	               *r = n->right;
		node_t *splice = (l != NULL) ? l : r;
		node_t *parentl = parent->left,
		       *parentr = parent->right;
	
		if (parentl != n && parentr != n) return 0;
		if (l != NULL && r != NULL) return 0;
		if (parentl == n) parent->left = splice;
		else              parent->right = splice;
		if (splice) splice->parent = parent;
	
		n->version = UNLINKED;
		return 1;
	}
	//> `node` and `parent` must be locked before calling.
	node_t *rebalance_node(node_t *parent, node_t *n)
	{
		node_t *nl = n->left, *nr = n->right;
		int hn, hl, hr, hn_new, balance;
	
		if ((!nl || !nr) && n->value == MARKED_NODE) {
			if (attempt_node_unlink(parent, n)) return fix_node_height(parent);
			else                                return n;
		}
	
		hn = n->height;
		hl = NODE_HEIGHT(nl);
		hr = NODE_HEIGHT(nr);
		hn_new = 1 + MAX(hl, hr);
		balance = hl - hr;
	
		if (balance > 1) return rebalance_right(parent, n, nl, hr);
		if (balance < -1) return rebalance_left(parent, n, nr, hl);
		if (hn != hn_new) {
			n->height = hn_new;
			return fix_node_height(parent);
		}
		return NULL;
	}
	void fix_height_and_rebalance(node_t *node)
	{
		node_t *node_old;
		while (node && node->parent) {
			int condition = node_condition(node);
	
			if (condition == NOTHING_REQUIRED || node->version == UNLINKED) return;
	
			if (condition != UNLINK_REQUIRED && condition != REBALANCE_REQUIRED) {
				node_old = node;
				LOCK(&node_old->lock);
				node = fix_node_height(node);
				UNLOCK(&node_old->lock);
				continue;
			}
	
			node_t *parent = node->parent;
			LOCK(&parent->lock);
			if (parent->version == UNLINKED || node->parent != parent) {
				UNLOCK(&parent->lock);
				continue;
			}
			node_old = node;
			LOCK(&node_old->lock);
			node = rebalance_node(parent, node);
			UNLOCK(&node_old->lock);
			UNLOCK(&parent->lock);
		}
	}
	/*****************************************************************************/

	int attempt_insert(const K& key, const V& value, node_t *node, int dir,
	                   long long version)
	{
		LOCK(&node->lock);
		if (node->version != version || GET_CHILD_DIR(node, dir)) {
			UNLOCK(&node->lock);
			return RETRY;
		}
	
		node_t *new_node = new node_t(key, value, 1, 0, node);
		if (dir == LEFT) node->left  = new_node;
		else             node->right = new_node;
		UNLOCK(&node->lock);
	
		fix_height_and_rebalance(node);
	
		return 1;
	}
	
	int attempt_relink(node_t *node)
	{
		int ret;
		LOCK(&node->lock);
		if (node->version == UNLINKED) {
			ret = RETRY;
		} else if (node->value == MARKED_NODE) {
			node->value = NULL;
			ret = 1;
		} else {
			ret = 0;
		}
		UNLOCK(&node->lock);
		return ret;
	}
	
	int attempt_put(const K& key, const V& value, node_t *node, int dir,
	                long long version)
	{
		node_t *child;
		int next_dir, ret = RETRY;
		long long child_version;
	
		do {
			child = GET_CHILD_DIR(node, dir);
			SW_BARRIER();
	
			//> The node version has changed. Must retry.
			if (node->version != version) return RETRY;
	
			if (child == NULL) {
				ret = attempt_insert(key, value, node, dir, version);
			} else {
				next_dir = (key < child->key) ? LEFT : RIGHT;
				if (key == child->key) {
					ret = attempt_relink(child);
				} else {
					child_version = child->version;
					if (IS_SHRINKING(child_version)) {
						wait_until_not_changing(child);
					} else if (child_version != UNLINKED && child == GET_CHILD_DIR(node, dir)) {
						if (node->version != version) return RETRY;
						ret = attempt_put(key, value, child, next_dir, child_version);
					}
				}
			}
		} while (ret == RETRY);
	
		return ret;
	}
	
	int insert_helper(const K& key, const V& value)
	{
		int ret;
		assert((void *)value != MARKED_NODE);
		ret = attempt_put(key, value, root, RIGHT, 0);
		assert(ret != RETRY);
		return ret;
	}

	int total_paths;
	int min_path_len, max_path_len;
	int total_nodes, marked_nodes, parent_errors, locked_nodes;
	int avl_violations, bst_violations;
	int validate_rec(node_t *root, int _depth,
	                 const K& range_min, const K& range_max)
	{
		if (!root)
			return -1;
	
		if (root->value == MARKED_NODE) marked_nodes++;
		
		if (root->lock != 1) locked_nodes++;
	
		node_t *left = root->left;
		node_t *right = root->right;
	
		if (left && left->parent != root) parent_errors++;
		if (right && right->parent != root) parent_errors++;
	
		total_nodes++;
		_depth++;
	
		/* BST violation? */
		if (range_min != 0 && root->key < range_min)
			bst_violations++;
		if (range_max != 0 && root->key > range_max)
			bst_violations++;
	
		/* We found a path (a node with at least one sentinel child). */
		if (!left || !right) {
			total_paths++;
	
			if (_depth <= min_path_len)
				min_path_len = _depth;
			if (_depth >= max_path_len)
				max_path_len = _depth;
		}
	
		/* Check subtrees. */
		int lheight = -1, rheight = -1;
		if (left)
			lheight = validate_rec(left, _depth, range_min, root->key);
		if (right)
			rheight = validate_rec(right, _depth, root->key, range_max);
	
		/* AVL violation? */
		if (abs(lheight - rheight) > 1)
			avl_violations++;
	
		return MAX(lheight, rheight) + 1;
	}
	
	inline int validate_helper()
	{
		int check_avl = 0, check_bst = 0;
		int check = 0;
		total_paths = 0;
		min_path_len = 99999999;
		max_path_len = -1;
		total_nodes = 0;
		marked_nodes = 0;
		locked_nodes = 0;
		parent_errors = 0;
		avl_violations = 0;
		bst_violations = 0;
	
		validate_rec(root->right, 0, 0, 0);
	
		check_avl = (avl_violations == 0);
		check_bst = (bst_violations == 0);
		check = (check_avl && check_bst);
	
		printf("Validation:\n");
		printf("=======================\n");
		printf("  Valid AVL Tree: %s\n",
		       check ? "Yes [OK]" : "No [ERROR]");
		printf("  AVL Violation: %s\n",
		       check_avl ? "No [OK]" : "Yes [ERROR]");
		printf("  BST Violation: %s\n",
		       check_bst ? "No [OK]" : "Yes [ERROR]");
		printf("  Total nodes: %d ( %d Unmarked / %d Marked )\n", total_nodes,
		                             total_nodes - marked_nodes, marked_nodes);
		printf("  Parent errors: %d\n", parent_errors);
		printf("  Locked nodes: %d\n", locked_nodes);
		printf("  Total paths: %d\n", total_paths);
		printf("  Min/max paths length: %d/%d\n", min_path_len, max_path_len);
		printf("\n");
	
		return check;
	}

	#define CAN_UNLINK(n) ((n)->left == NULL || (n)->right == NULL)
	int attempt_rm_node(node_t *par, node_t *n)
	{
		if (n->value == MARKED_NODE) return 0;
	
		if (!CAN_UNLINK(n)) {
			//> Internal node, just mark it
			LOCK(&n->lock);
			if (n->version == UNLINKED || CAN_UNLINK(n)) {
				UNLOCK(&n->lock);
				return RETRY;
			}
			if (n->value != MARKED_NODE) {
				n->value = MARKED_NODE;
				UNLOCK(&n->lock);
				return 1;
			} else {
				UNLOCK(&n->lock);
				return 0;
			}
		}
	
		//> External node, can remove it from the tree
		LOCK(&par->lock);
		if (par->version == UNLINKED || n->parent != par) {
			UNLOCK(&par->lock);
			return RETRY;
		}
		LOCK(&n->lock);
		if (n->version == UNLINKED || par->version == UNLINKED || n->parent != par) {
			UNLOCK(&n->lock);
			UNLOCK(&par->lock);
			return RETRY;
		}
	
		if (n->value== MARKED_NODE) {
			UNLOCK(&n->lock);
			UNLOCK(&par->lock);
			return 0;
		}
	
		n->value = MARKED_NODE;
		if (CAN_UNLINK(n)) {
			node_t *c = (n->left == NULL) ? n->right : n->left;
			if (par->left == n) par->left = c;
			else                par->right = c;
			if (c != NULL) c->parent = par;
			n->version = UNLINKED;
		}
		UNLOCK(&n->lock);
		UNLOCK(&par->lock);
	
		fix_height_and_rebalance(par);
	
		return 1;
	}
	
	int attempt_remove(const K& key, node_t *node, int dir, long long version)
	{
		node_t *child;
		int next_dir, ret = RETRY;
		long long child_version;
	
		do {
			child = GET_CHILD_DIR(node, dir);
			SW_BARRIER();
	
			//> The node version has changed. Must retry.
			if (node->version != version) return RETRY;
	
			if (child == NULL) return 0;
	
			next_dir = (key < child->key) ? LEFT : RIGHT;
			if (key == child->key) {
				ret = attempt_rm_node(node, child);
			} else {
				child_version = child->version;
				if (IS_SHRINKING(child_version)) {
					wait_until_not_changing(child);
				} else if (child_version != UNLINKED && child == GET_CHILD_DIR(node, dir)) {
					if (node->version != version) return RETRY;
					ret = attempt_remove(key, child, next_dir, child_version);
				}
			}
		} while (ret == RETRY);
	
		return ret;
	}
	
	int delete_helper(const K& key)
	{
		int ret;
		ret = attempt_remove(key, root, RIGHT, 0);
		assert(ret != RETRY);
		return ret;
	}
	
	int attempt_update(const K& key, const V& value, node_t *node, int dir,
	                   long long version)
	{
		node_t *child;
		int next_dir, ret = RETRY;
		long long child_version;
	
		do {
			child = GET_CHILD_DIR(node, dir);
			SW_BARRIER();
	
			//> The node version has changed. Must retry.
			if (node->version != version) return RETRY;
	
			if (child == NULL) {
				ret = attempt_insert(key, value, node, dir, version);
				break;
			}
	
			next_dir = (key < child->key) ? LEFT : RIGHT;
			if (key == child->key) {
				if (child->value == MARKED_NODE) {
					ret = attempt_relink(child);
				} else {
					ret = attempt_rm_node(node, child);
					if (ret != RETRY) ret += 2;
				}
			} else {
				child_version = child->version;
				if (IS_SHRINKING(child_version)) {
					wait_until_not_changing(child);
				} else if (child_version != UNLINKED && child == GET_CHILD_DIR(node, dir)) {
					if (node->version != version) return RETRY;
					ret = attempt_update(key, value, child, next_dir, child_version);
				}
			}
		} while (ret == RETRY);
	
		return ret;
	}
	
	int update_helper(const K& key, const V& value)
	{
		int ret;
		ret = attempt_update(key, value, root, RIGHT, 0);
		assert(ret != RETRY);
		return ret;
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
	
		std::cout << root->key << "[" << root->height << "](" << root << ")\n";
	
		print_rec(root->left, level + 1);
	}
	
	void print_struct()
	{
		if (root->right == NULL)
			std::cout << "[empty]";
		else
			print_rec(root->right, 0);
		std::cout << "\n";
	}
	/******************************************************************************/
};

#define BST_AVL_BRONSON_TEMPL template<typename K, typename V>
#define BST_AVL_BRONSON_FUNCT bst_avl_bronson<K,V>

BST_AVL_BRONSON_TEMPL
bool BST_AVL_BRONSON_FUNCT::contains(const int tid, const K& key)
{
	const V = ret lookup_helper(key);
	return ret != this->NO_VALUE;
}

BST_AVL_BRONSON_TEMPL
const std::pair<V,bool> BST_AVL_BRONSON_FUNCT::find(const int tid, const K& key)
{
	const V ret = lookup_helper(key);
	return std::pair<V,bool>(ret, ret != this->NO_VALUE);
}

BST_AVL_BRONSON_TEMPL
int BST_AVL_BRONSON_FUNCT::rangeQuery(const int tid, const K& lo, const K& hi,
                      std::vector<std::pair<K,V>> kv_pairs)
{
	return 0;
}

BST_AVL_BRONSON_TEMPL
const V BST_AVL_BRONSON_FUNCT::insert(const int tid, const K& key, const V& val)
{
	return insertIfAbsent(tid, key, val);
}

BST_AVL_BRONSON_TEMPL
const V BST_AVL_BRONSON_FUNCT::insertIfAbsent(const int tid, const K& key, const V& val)
{
	int ret = insert_helper(key, val);
	if (ret == 1) return NULL;
	else return (void *)1;
}

BST_AVL_BRONSON_TEMPL
const std::pair<V,bool> BST_AVL_BRONSON_FUNCT::remove(const int tid, const K& key)
{
	int ret = delete_helper(key);
	if (ret == 0) return std::pair<V,bool>(NULL, false);
	else return std::pair<V,bool>((void*)1, true);
}

BST_AVL_BRONSON_TEMPL
bool BST_AVL_BRONSON_FUNCT::validate()
{
	return validate_helper();
}
