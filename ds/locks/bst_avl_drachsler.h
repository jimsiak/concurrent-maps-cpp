/**
 * A partially external relaxed-balanced AVL tree.
 * Paper:
 *    Practical concurrent binary search trees via logical ordering, Drachsler et. al, PPoPP 2014
 **/

/**
 * NOTES:
 * The `value` field of `node_t` is used to mark a node, so we have a problem
 * with having V as a value type.
 * Actually using `value` to mark nodes did indeed create a problem.
 * At 17/9/2021 while debugging I encountered the problem when a node with value
 * 0xFF (dec: 255) was falsely regarded as a marked node. To bypass the
 * problem I set MARKED_NODE to 0xffffffff and added an assert statement when
 * inserting a node with `value` equal to MARKED_NODE.
 *
 * This tree needs MIN_KEY and MAX_KEY, which should never be inserted in the
 * tree. At 17/9/2021 I encountered the bug where with MIN_KEY set to 0 and
 * trying to insert 0, the warmup phase was getting stuck in an infinite
 * retry loop in `insert_helper`.
 **/

#pragma once

#include <climits>
#include "../map_if.h"
#include "Log.h"
#include "lock.h"

#define MAX_KEY_DRACHSLER MAX_KEY
#define MIN_KEY_DRACHSLER MIN_KEY

#define DRACHSLER_MARK(n) ((n)->marked = true)
#define DRACHSLER_IS_MARKED(n) ((n)->marked)

#define MAX(a,b) ( (a) >= (b) ? (a) : (b) )
#define ABS(a) ( ((a) >= 0) ? (a) : -(a) )

template <typename K, typename V>
class bst_avl_drachsler: public Map<K,V> {
public:
	bst_avl_drachsler(const K _NO_KEY, const V _NO_VALUE, const int numProcesses)
	  : Map<K,V>(_NO_KEY, _NO_VALUE)
	{
		node_t *parent = new node_t(MIN_KEY_DRACHSLER, 0);
		node_t *n = new node_t(MAX_KEY_DRACHSLER, 0);
		n->pred = parent;
		n->succ = parent;
		n->parent = parent;
		parent->right = n;
		parent->succ = n;
		root = n;
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
	char *name() { return "BST AVL Drachsler"; }

	void print() { print_struct(); };
//	unsigned long long size() { return size_rec(root); };

private:

	struct node_t {
		K key;
		V value;
	
		short int lheight, rheight;

		bool marked;

		node_lock_t tree_lock, succ_lock;
	
		node_t *left, *right, *parent;
		node_t *succ, *pred;
		char padding[32];

		node_t(K key, V value) {
			this->key = key;
			this->value = value;
			this->lheight = lheight;
			this->rheight = rheight;
			this->marked = false;
			this->parent = this->succ = this->pred = NULL;
			this->right = this->left = NULL;
			INIT_LOCK(&this->tree_lock);
			INIT_LOCK(&this->succ_lock);
		}

	};

	node_t *root;

private:

	node_t *search(const K& k)
	{
		node_t *child;
		node_t *n = root;
		K curr_key;
	
		while (1) {
			curr_key = n->key;
			if (curr_key == k) return n;
			child = (curr_key < k) ? n->right : n->left;
			if (child == NULL) return n;
			n = child;
		}
	}
	
	const V lookup_helper(const K& k)
	{
		node_t *n = search(k);
		while (n->key > k && n->pred->key >= k) n = n->pred;
		while (n->key < k && n->succ->key <= k) n = n->succ;
		if (n->key == k && !DRACHSLER_IS_MARKED(n)) return n->value;
		else return this->NO_VALUE;
	}

	/*****************************************************************************/
	/*                                REBALANCING                                */
	/*****************************************************************************/
	void rotate(node_t *child, node_t *n, node_t *parent, char left_rotation)
	{
		update_child(parent, n, child);
		n->parent = child;
		if (left_rotation) {
			update_child(n, child, child->left);
			child->left = n;
			n->rheight = child->lheight;
			child->lheight = MAX(n->lheight, n->rheight) + 1;
		} else {
			update_child(n, child, child->right);
			child->right = n;
			n->lheight = child->rheight;
			child->rheight = MAX(n->lheight, n->rheight) + 1;
		}
	}
	//> Returns 1 if height has changed, 0 otherwise
	char update_height(node_t *child, node_t *node, char is_left)
	{
		short int new_height, old_height;
		new_height = (child == NULL) ? 0 : MAX(child->lheight, child->rheight) + 1;
		old_height = is_left ? node->lheight : node->rheight;
		if (old_height != new_height) {
			if (is_left) node->lheight = new_height;
			else         node->rheight = new_height;
		}
		return (old_height != new_height);
	}
	//> `node` and `child` must be tree locked
	void rebalance(node_t *node, node_t *child)
	{
		char is_left, updated;
		short int balance, ch_balance;
		node_t *grandchild, *parent = NULL;
	
		//> At the start of each iteration `node` and `child` are locked (if NULL).
		while (1) {
			//> Rebalance has reached the root.
			if (node == root || node == root->parent)
				goto unlock_and_out;
	
			is_left = (node->left == child);
			updated = update_height(child, node, is_left);
			if (!child && !node->left && !node->right) {
				node->lheight = 0;
				node->rheight = 0;
				updated = 1;
			}
			balance = node->lheight - node->rheight;
			if (!updated && ABS(balance) < 2) goto unlock_and_out;
	
			//> `child` is not the appropriate, release the previous one and
			//>  lock the correct one.
			if ((is_left && balance <= -2) || (!is_left && balance >= 2)) {
				if (child) UNLOCK(&child->tree_lock);
				is_left = !is_left;
				child = is_left ? node->left : node->right;
				if (TRYLOCK(&child->tree_lock) != 0) {
					UNLOCK(&node->tree_lock);
					return; // XXX no restart here as in the paper
				}
			}
	
			if (ABS(balance) >= 2) { // Need to rebalance
				//> First rotation if needed
				ch_balance = (child == NULL) ? 0 : child->lheight - child->rheight;
				if ((is_left && ch_balance < 0) || (!is_left && ch_balance > 0)) {
					grandchild = is_left ? child->right : child->left;
					if (!grandchild) goto unlock_and_out; // JIMSIAK
					if (TRYLOCK(&grandchild->tree_lock) != 0) {
						goto unlock_and_out; // XXX no restart here as in the paper
					}
					rotate(grandchild, child, node, is_left);
					UNLOCK(&child->tree_lock);
					child = grandchild;
				}
	
				//> Second (or first) rotation
				parent = lock_parent(node);
				rotate(child, node, parent, !is_left);
	
				UNLOCK(&node->tree_lock);
				node = parent;
			} else { // No rotations needed but the height has been updated
				if (child) UNLOCK(&child->tree_lock);
				child = node;
				node = lock_parent(node);
			}
		}
	
	unlock_and_out:
		if (child) UNLOCK(&child->tree_lock);
		if (node) UNLOCK(&node->tree_lock);
		return;
	}
	/*****************************************************************************/
	
	//> Returs n's parent node, and locks its `tree_lock` as well.
	node_t *lock_parent(node_t *n)
	{
		node_t *p;
		while (1) {
			p = n->parent;
			LOCK(&p->tree_lock);
			if (n->parent == p && !DRACHSLER_IS_MARKED(p)) return p;
			UNLOCK(&p->tree_lock);
		}
	}
	
	node_t *choose_parent(node_t *p, node_t *s, node_t *first_cand)
	{
		node_t *candidate = (first_cand == p || first_cand == s) ? first_cand : p;
	
		while (1) {
			LOCK(&candidate->tree_lock);
			if (candidate == p) {
				if (!candidate->right) return candidate;
				UNLOCK(&candidate->tree_lock);
				candidate = s;
			} else {
				if (!candidate->left) return candidate;
				UNLOCK(&candidate->tree_lock);
				candidate = p;
			}
		}
	}
	
	// `p` is tree locked when called
	void insert_to_tree(node_t *p, node_t *n)
	{
		n->parent = p;
		if (p->key < n->key) {
			p->right = n;
			p->rheight = 1;
		} else {
			p->left = n;
			p->lheight = 1;
		}
		rebalance(lock_parent(p), p);
	}
	
	const V insert_helper(const K& k, const V& v)
	{
		assert((void *)v != (void *)0xFFFFFFFFLLU);

		node_t *node, *p, *s, *new_node, *parent;
	
		while(1) {
			node = search(k);
			p = (node->key >= k) ? node->pred : node;
			LOCK(&p->succ_lock);
			s = p->succ;
	
			if (k > p->key && k <= s->key && !DRACHSLER_IS_MARKED(p)) {
				//> Key already in the tree
				if (s->key == k) {
					UNLOCK(&p->succ_lock);
					return s->value;
				}
				//> Ordering insertion
				new_node = new node_t(k, v);
				parent = choose_parent(p, s, node);
				new_node->succ = s;
				new_node->pred = p;
				new_node->parent = parent;
				s->pred = new_node;
				p->succ = new_node;
				UNLOCK(&p->succ_lock);
				//> Tree Layout insertion
				insert_to_tree(parent, new_node);
				return this->NO_VALUE;
			}
			UNLOCK(&p->succ_lock);
		}
	}

	//> Acquires the required `tree_lock` locks.
	//> - If `n` has 0 or 1 child: `n->parent`, `n` and `n->NOT_NULL_CHILD`
	//>   are locked.
	//> - If `n` has 2 children:  `n->parent`, `n`, `n->succ->parent`,
	//>   `n->succ` and `n->succ->right` are locked.
	int acquire_tree_locks(node_t *n)
	{
		node_t *s, *parent, *sp;
		long long retries = -1;
	
		while (1) {
			retries++;
	
			// JIMSIAK, this is necessary, otherwise performance collapses
			volatile int sum = 0;
			int i; for (i=0; i < retries * 9; i++) sum++;
	
			LOCK(&n->tree_lock);
			parent = lock_parent(n); // NEW
	
			//> Node has ONE or NO child
			if (!n->left || !n->right) {
				if (n->right) {
					if (TRYLOCK(&n->right->tree_lock) != 0) {
						UNLOCK(&parent->tree_lock);
						UNLOCK(&n->tree_lock);
						continue;
					}
				} else if (n->left) {
					if (TRYLOCK(&n->left->tree_lock) != 0) {
						UNLOCK(&parent->tree_lock);
						UNLOCK(&n->tree_lock);
						continue;
					}
				}
				return 0;
			}
	
			//> Node has TWO children
			s = n->succ;
			sp = NULL;
			if (s->parent != n) {
				sp = s->parent;
				if (TRYLOCK(&sp->tree_lock) != 0) {
					UNLOCK(&parent->tree_lock);
					UNLOCK(&n->tree_lock);
					continue;
				}
				if (sp != s->parent || DRACHSLER_IS_MARKED(sp)) {
					UNLOCK(&sp->tree_lock);
					UNLOCK(&parent->tree_lock);
					UNLOCK(&n->tree_lock);
					continue;
				}
			}
	
			if (TRYLOCK(&s->tree_lock) != 0) {
				if (sp) UNLOCK(&sp->tree_lock);
				UNLOCK(&parent->tree_lock);
				UNLOCK(&n->tree_lock);
				continue;
			}
	
			if (s->right) {
				if (TRYLOCK(&s->right->tree_lock)) {
					UNLOCK(&s->tree_lock);
					if (sp) UNLOCK(&sp->tree_lock);
					UNLOCK(&parent->tree_lock);
					UNLOCK(&n->tree_lock);
					continue;
				}
			}
	
			return 1;
		}
	}

	void update_child(node_t *parent, node_t *old_ch, node_t *new_ch)
	{
		if (parent->left == old_ch) parent->left = new_ch;
		else                        parent->right = new_ch;
		if (new_ch != NULL) new_ch->parent = parent;
	}

	void remove_from_tree(node_t *n, int has_two_children)
	{
		node_t *child, *parent, *s, *sparent, *schild;
	
		if (!has_two_children) {
			child = (n->right == NULL) ? n->left : n->right;
			parent = n->parent;
			update_child(parent, n, child);
			UNLOCK(&n->tree_lock);
			rebalance(parent, child);
			return;
		} else {
			parent = n->parent;
			s = n->succ;
			schild = s->right;
			sparent = s->parent;
			update_child(sparent, s, schild);
			s->left = n->left;
			s->right = n->right;
			s->lheight = n->lheight;
			s->rheight = n->rheight;
			n->left->parent = s;
			if (n->right) n->right->parent = s;
			update_child(parent, n, s);
			UNLOCK(&parent->tree_lock);
			UNLOCK(&n->tree_lock);
			if (sparent == n) sparent = s;
			else UNLOCK(&s->tree_lock);
			rebalance(sparent, schild);
			return;
		}
	}
	
	const V delete_helper(const K& k)
	{
		node_t *node, *p, *s, *s_succ;
		int has_two_children;
	
		while (1) {
			node = search(k);
			p = (node->key >= k) ? node->pred : node;
			LOCK(&p->succ_lock);
			s = p->succ;
	
			if (k > p->key && k <= s->key && !DRACHSLER_IS_MARKED(p)) {
				if (s->key > k) {
					UNLOCK(&p->succ_lock);
					return this->NO_VALUE;
				}
				LOCK(&s->succ_lock);
				has_two_children = acquire_tree_locks(s);
				const V ret = s->value;
				DRACHSLER_MARK(s);
				s_succ = s->succ;
				s_succ->pred = p;
				p->succ = s_succ;
				UNLOCK(&s->succ_lock);
				UNLOCK(&p->succ_lock);
				remove_from_tree(s, has_two_children);
				return ret; 
			}
			UNLOCK(&p->succ_lock);
		}
	}
	
	int update_helper(const K& k, const V& v)
	{
		node_t *node, *p, *s, *new_node, *parent, *s_succ;
		int has_two_children, op_is_insert = -1;
	
		while(1) {
			node = search(k);
			p = (node->key >= k) ? node->pred : node;
			LOCK(&p->succ_lock);
			s = p->succ;
	
			if (k > p->key && k <= s->key && !DRACHSLER_IS_MARKED(p)) {
				if (op_is_insert == -1) {
					if (s->key == k) op_is_insert = 0;
					else             op_is_insert = 1;
				}
	
				if (op_is_insert) {
					//> Key already in the tree
					if (s->key == k) {
						UNLOCK(&p->succ_lock);
						return 0;
					}
					//> Ordering insertion
					new_node = new node_t(k, v);
					parent = choose_parent(p, s, node);
					new_node->succ = s;
					new_node->pred = p;
					new_node->parent = parent;
					s->pred = new_node;
					p->succ = new_node;
					UNLOCK(&p->succ_lock);
					//> Tree Layout insertion
					insert_to_tree(parent, new_node);
					return 1;
				} else {
					if (s->key > k) {
						UNLOCK(&p->succ_lock);
						return 2;
					}
					LOCK(&s->succ_lock);
					has_two_children = acquire_tree_locks(s);
					DRACHSLER_MARK(s);
					s_succ = s->succ;
					s_succ->pred = p;
					p->succ = s_succ;
					UNLOCK(&s->succ_lock);
					UNLOCK(&p->succ_lock);
					remove_from_tree(s, has_two_children);
					return 3; 
				}
			}
			UNLOCK(&p->succ_lock);
		}
	}

	int bst_violations, avl_violations;
	int total_paths, total_nodes;
	int min_path_len, max_path_len;
	int wrong_heights;
	int validate_rec(node_t *root, int _th)
	{
		if (!root)
			return -1;
	
		node_t *left = root->left;
		node_t *right = root->right;
	
		total_nodes++;
		_th++;
	
		/* BST violation? */
		if (left && left->key >= root->key)
			bst_violations++;
		if (right && right->key <= root->key)
			bst_violations++;
	
		/* We found a path (a node with at least one sentinel child). */
		if ((!left || !right) && (root->key != MAX_KEY_DRACHSLER && root->key != MIN_KEY_DRACHSLER)) {
			total_paths++;
	
			if (_th <= min_path_len)
				min_path_len = _th;
			if (_th >= max_path_len)
				max_path_len = _th;
		}
	
		/* Check subtrees. */
		int lheight = 0, rheight = 0;
		if (left) lheight = validate_rec(left, _th);
		if (right) rheight = validate_rec(right, _th);
	
		if (lheight != root->lheight) wrong_heights++;
		if (rheight != root->rheight) wrong_heights++;
	
		/* AVL violation? */
		if (abs(lheight - rheight) > 1)
			avl_violations++;
	
		return MAX(lheight, rheight) + 1;
	}
	
	inline int validate_helper()
	{
		int check, check_bst, check_avl;
		total_paths = 0;
		min_path_len = 99999999;
		max_path_len = -1;
		total_nodes = 0;
		avl_violations = 0;
		bst_violations = 0;
		wrong_heights = 0;
	
		validate_rec(root->left, 0);
	
		check_avl = (avl_violations == 0 && wrong_heights == 0);
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
		printf("  Tree size: %8d\n", total_nodes);
		printf("  Total paths: %d\n", total_paths);
		printf("  Wrong node heights: %d\n", wrong_heights);
		printf("  Min/max paths length: %d/%d\n", min_path_len, max_path_len);
		printf("\n");
	
		return check;
	}
	
	/*********************    FOR DEBUGGING ONLY    *******************************/
	void print_rec(node_t *root, int level)
	{
		int i;
	
		if (root) print_rec(root->right, level + 1);
	
		for (i = 0; i < level; i++)
			std::cout << "|--";
	
		if (!root) {
			std::cout << "NULL\n";
			return;
		}
	
		std::cout << root->key << " [" << root->lheight << ", " << root->rheight
		          << "]" << ((root->tree_lock < 1) ? " Tlock" : "") << "\n";
	
		print_rec(root->left, level + 1);
	}
	
	void print_struct()
	{
		if (root->left == NULL) std::cout << "[empty]";
		else print_rec(root->left, 0);
		std::cout << "\n";
	}
	/******************************************************************************/
};

#define BST_AVL_DRACHSLER_TEMPL template<typename K, typename V>
#define BST_AVL_DRACHSLER_FUNCT bst_avl_drachsler<K,V>

BST_AVL_DRACHSLER_TEMPL
bool BST_AVL_DRACHSLER_FUNCT::contains(const int tid, const K& key)
{
	const V ret = lookup_helper(key);
	return ret != this->NO_VALUE;
}

BST_AVL_DRACHSLER_TEMPL
const std::pair<V,bool> BST_AVL_DRACHSLER_FUNCT::find(const int tid, const K& key)
{
	const V ret = lookup_helper(key);
	return std::pair<V,bool>(ret, ret != this->NO_VALUE);
}

BST_AVL_DRACHSLER_TEMPL
int BST_AVL_DRACHSLER_FUNCT::rangeQuery(const int tid, const K& lo, const K& hi,
                      std::vector<std::pair<K,V>> kv_pairs)
{
	return 0;
}

BST_AVL_DRACHSLER_TEMPL
const V BST_AVL_DRACHSLER_FUNCT::insert(const int tid, const K& key, const V& val)
{
	return insertIfAbsent(tid, key, val);
}

BST_AVL_DRACHSLER_TEMPL
const V BST_AVL_DRACHSLER_FUNCT::insertIfAbsent(const int tid, const K& key, const V& val)
{
	return insert_helper(key, val);
}

BST_AVL_DRACHSLER_TEMPL
const std::pair<V,bool> BST_AVL_DRACHSLER_FUNCT::remove(const int tid, const K& key)
{
	const V ret = delete_helper(key);
	return std::pair<V,bool>(ret, ret != this->NO_VALUE);
}

BST_AVL_DRACHSLER_TEMPL
bool BST_AVL_DRACHSLER_FUNCT::validate()
{
	return validate_helper();
}
