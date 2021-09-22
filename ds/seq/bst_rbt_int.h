/**
 * An internal Red-Black tree
 **/

#pragma once

#include "../map_if.h"
#include "Log.h"

#define MAX_HEIGHT 50
#define IS_BLACK(node) ( !(node) || (node)->color == BLACK )
#define IS_RED(node) ( !IS_BLACK(node) )

template <typename K, typename V>
class bst_rbt_int : public Map<K,V> {
public:
	bst_rbt_int(const K _NO_KEY, const V _NO_VALUE, const int numProcesses)
	  : Map<K,V>(_NO_KEY, _NO_VALUE)
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
	char *name() { return "BST Red-Black Internal"; }

	void print() {};
//	unsigned long long size() { return size_rec(root); };

private:

	typedef enum {
		RED = 0,
		BLACK
	} color_t;

	struct node_t {
		K key;
		V value;

		color_t color;
		node_t *left, *right;

		node_t(K key, V value, color_t color) {
			this->key = key;
			this->value = value;
			this->color = color;
			this->left = NULL;
			this->right = NULL;
		}
	};

	node_t *root;

	inline node_t *rotate_left(node_t *node)
	{
		assert(node != NULL && node->right != NULL);
	
		node_t *node_right = node->right;
	
		node->right = node->right->left;
		node_right->left = node;
	
		return node_right;
	}

	inline node_t *rotate_right(node_t *node)
	{
		assert(node != NULL && node->left != NULL);
	
		node_t *node_left = node->left;
	
		node->left = node->left->right;
		node_left->right = node;
	
		return node_left;
	}

	/**
	 * Traverses the tree as dictated by `key`.
	 * When returning, `leaf` is either NULL (key not found) or the leaf that
	 * contains `key`. `parent` is either leaf's parent (if `leaf` != NULL) or
	 * the node that will be the parent of the inserted node.
	 * In the case of an empty tree both `parent` and `leaf` are NULL.
	 **/
	inline void traverse(const K& key, node_t **parent, node_t **leaf)
	{
		*parent = NULL;
		*leaf = root;
	
		while (*leaf) {
			K leaf_key = (*leaf)->key;
			if (leaf_key == key)
				return;
	
			*parent = *leaf;
			*leaf = (key < leaf_key) ? (*leaf)->left : (*leaf)->right;
		}
	}

	inline void traverse_with_stack(const K& key, node_t *node_stack[MAX_HEIGHT],
	                                int *stack_top)
	{
		node_t *parent, *leaf;
	
		parent = NULL;
		leaf = root;
		*stack_top = -1;
	
		while (leaf) {
			node_stack[++(*stack_top)] = leaf;
	
			K leaf_key = leaf->key;
			if (leaf_key == key)
				return;
	
			parent = leaf;
			leaf = (key < leaf_key) ? leaf->left : leaf->right;
		}
	}

	int lookup_helper(const K& key)
	{
		node_t *parent, *leaf;
		traverse(key, &parent, &leaf);
		return (leaf != NULL);
	}

	void insert_rebalance(const K& key, node_t *node_stack[MAX_HEIGHT],
	                      int stack_top)
	{
		node_t *parent, *grandparent, *grandgrandparent, *uncle;
	
		while (1) {
			if (stack_top <= 0) {
				root->color = BLACK;
				break;
			}
	
			parent = node_stack[stack_top--];
			if (IS_BLACK(parent))
				break;
	
			grandparent = node_stack[stack_top--];
			if (key < grandparent->key) {
				uncle = grandparent->right;
				if (IS_RED(uncle)) {
					parent->color = BLACK;
					uncle->color = BLACK;
					grandparent->color = RED;
					continue;
				}
	
				if (key < parent->key) {
					if (stack_top == -1) {
						root = rotate_right(grandparent);
					} else {
						grandgrandparent = node_stack[stack_top];
						if (key < grandgrandparent->key)
							grandgrandparent->left = rotate_right(grandparent);
						else
							grandgrandparent->right = rotate_right(grandparent);
					}
					parent->color = BLACK;
					grandparent->color = RED;
				} else {
					grandparent->left = rotate_left(parent);
					if (stack_top == -1) {
						root = rotate_right(grandparent);
						root->color = BLACK;
					} else {
						grandgrandparent = node_stack[stack_top];
						if (key < grandgrandparent->key) {
							grandgrandparent->left = rotate_right(grandparent);
							grandgrandparent->left->color = BLACK;
						} else {
							grandgrandparent->right = rotate_right(grandparent);
							grandgrandparent->right->color = BLACK;
						}
					}
					grandparent->color = RED;
				}
				break;
			} else {
				uncle = grandparent->left;
				if (IS_RED(uncle)) {
					parent->color = BLACK;
					uncle->color = BLACK;
					grandparent->color = RED;
					continue;
				}
	
				if (key > parent->key) {
					if (stack_top == -1) {
						root = rotate_left(grandparent);
					} else {
						grandgrandparent = node_stack[stack_top];
						if (key < grandgrandparent->key)
							grandgrandparent->left = rotate_left(grandparent);
						else
							grandgrandparent->right = rotate_left(grandparent);
					}
					parent->color = BLACK;
					grandparent->color = RED;
				} else {
					grandparent->right = rotate_right(parent);
					if (stack_top == -1) {
						root = rotate_left(grandparent);
						root->color = BLACK;
					} else {
						grandgrandparent = node_stack[stack_top];
						if (key < grandgrandparent->key) {
							grandgrandparent->left = rotate_left(grandparent);
							grandgrandparent->left->color = BLACK;
						} else {
							grandgrandparent->right = rotate_left(grandparent);
							grandgrandparent->right->color = BLACK;
						}
					}
					grandparent->color = RED;
				}
				break;
			}
		}
	}

	int do_insert(const K& key, const V& value,
	              node_t *node_stack[MAX_HEIGHT], int stack_top)
	{
		// Empty tree
		if (stack_top == -1) {
			root = new node_t(key, value, RED);
			return 1;
		}
	
		node_t *parent = node_stack[stack_top];
		if (key == parent->key)     return 0;
		else if (key < parent->key) parent->left = new node_t(key, value, RED);
		else                        parent->right = new node_t(key, value, RED);
		return 1;
	}

	const V insert_helper(const K& key, const V& value)
	{
		node_t *node_stack[MAX_HEIGHT];
		int stack_top;
	
		traverse_with_stack(key, node_stack, &stack_top);
		int ret = do_insert(key, value, node_stack, stack_top);
		if (ret == 0) return node_stack[stack_top]->value;
	
		insert_rebalance(key, node_stack, stack_top);
		return this->NO_VALUE;
	}

	int do_delete(const K& key, node_t *node_stack[MAX_HEIGHT],
	              int *stack_top, color_t *deleted_node_color, int *succ_key)
	{
		node_t *leaf, *parent, *curr, *original_node = NULL;
	
		*succ_key = key;
	
		// Empty tree
		if (*stack_top == -1) return 0;
	
		leaf = node_stack[*stack_top];
		if (key != leaf->key) return 0;
	
		// If node has two children, find successor
		if (leaf->left && leaf->right) {
			original_node = leaf;
			curr = leaf->right;
			node_stack[++(*stack_top)] = curr;
			while (curr->left) {
				curr = curr->left;
				node_stack[++(*stack_top)] = curr;
			}
		}
	
		leaf = node_stack[*stack_top];
		if (*stack_top == 0) {
			if (!leaf->left) root = leaf->right;
			else             root = leaf->left;
		} else {
			parent = node_stack[*stack_top - 1];
			if (key < parent->key) {
				if (!leaf->left) parent->left = leaf->right;
				else             parent->left = leaf->left;
			} else {
				if (!leaf->left) parent->right = leaf->right;
				else             parent->right = leaf->left;
			}
		}
		
		// Add the non-NULL child of leaf (or NULL, if leaf has 0 children)
		// in the stack. We do this for the rebalancing that follows.
		node_stack[*stack_top] = leaf->left;
		if (!leaf->left) node_stack[*stack_top] = leaf->right;
	
		if (original_node) {
			original_node->key = leaf->key;
			*succ_key = leaf->key;
		}
	
		*deleted_node_color = leaf->color;
	
		return 1;
	}

	void delete_rebalance(const K& key, node_t *node_stack[MAX_HEIGHT],
	                      int stack_top)
	{
		node_t *curr, *parent, *gparent, *sibling;
	
		while (1) {
			curr = node_stack[stack_top--];
			if (IS_RED(curr)) {
				curr->color = BLACK;
				return;
			}
			if (stack_top == -1) {
				if (IS_RED(curr)) curr->color = BLACK;
				return;
			}
	
			parent = node_stack[stack_top];
			
			if (key < parent->key) { // `curr` is left child
				sibling = parent->right;
				if (IS_RED(sibling)) { // CASE 1
					sibling->color = BLACK;
					parent->color = RED;
					if (stack_top - 1 == -1) {
						root = rotate_left(parent);
	
						// CASES 2, 3 and/or 4 below need to get the right stack
						node_stack[0] = sibling;
						node_stack[1] = parent;
						stack_top = 1;
					} else {
						gparent = node_stack[stack_top - 1];
						if (key < gparent->key) gparent->left = rotate_left(parent);
						else                   gparent->right = rotate_left(parent);
	
						// CASES 2, 3 and/or 4 below need to get the right gparent
						node_stack[stack_top - 1] = sibling;
					}
					sibling = parent->right;
				}
	
				if (IS_BLACK(sibling->left) && IS_BLACK(sibling->right)) { // CASE 2
					sibling->color = RED;
					continue;
				} else {
					if (IS_BLACK(sibling->right)) { // CASE 4
						sibling->left->color = BLACK;
						sibling->color = RED;
						parent->right = rotate_right(sibling);
						sibling = parent->right;
					}
					// CASE 3
					sibling->color = parent->color;
					parent->color = BLACK;
					sibling->right->color = BLACK;
					if (stack_top - 1 == -1) {
						root = rotate_left(parent);
					} else {
						gparent = node_stack[stack_top - 1];
						if (key < gparent->key) gparent->left = rotate_left(parent);
						else                   gparent->right = rotate_left(parent);
					}
					break;
				}
			} else { // `curr` is right child
				sibling = parent->left;
				if (IS_RED(sibling)) { // CASE 1
					sibling->color = BLACK;
					parent->color = RED;
					if (stack_top - 1 == -1) {
						root = rotate_right(parent);
	
						// CASES 2, 3 and/or 4 below need to get the right stack
						node_stack[0] = sibling;
						node_stack[1] = parent;
						stack_top = 1;
					} else {
						gparent = node_stack[stack_top - 1];
						if (key < gparent->key) gparent->left = rotate_right(parent);
						else                   gparent->right = rotate_right(parent);
	
						// CASES 2, 3 and/or 4 below need to get the right gparent
						node_stack[stack_top - 1] = sibling;
					}
					sibling = parent->left;
				}
	
				if (IS_BLACK(sibling->left) && IS_BLACK(sibling->right)) { // CASE 2
					sibling->color = RED;
					continue;
				} else {
					if (IS_BLACK(sibling->left)) { // CASE 4
						sibling->right->color = BLACK;
						sibling->color = RED;
						parent->left = rotate_left(sibling);
						sibling = parent->left;
					}
					// CASE 3
					sibling->color = parent->color;
					parent->color = BLACK;
					sibling->left->color = BLACK;
					if (stack_top - 1 == -1) {
						root = rotate_right(parent);
					} else {
						gparent = node_stack[stack_top - 1];
						if (key < gparent->key) gparent->left = rotate_right(parent);
						else                   gparent->right = rotate_right(parent);
					}
					break;
				}
			}
		}
	}

	const V delete_helper(const K& key)
	{
		node_t *node_stack[MAX_HEIGHT];
		int stack_top;
		color_t deleted_node_color;
		int succ_key;
	
		traverse_with_stack(key, node_stack, &stack_top);
		const V del_val = node_stack[stack_top]->value;
		int ret = do_delete(key, node_stack, &stack_top, &deleted_node_color,
		                  &succ_key);
		if (ret == 0) return this->NO_VALUE;
	
		if (deleted_node_color == BLACK)
			delete_rebalance(succ_key, node_stack, stack_top);
	
		return del_val;
	}

	int key_in_min_path, key_in_max_path;
	int bh;
	int paths_with_bh_diff;
	int total_paths;
	int min_path_len, max_path_len;
	int total_nodes, red_nodes, black_nodes;
	int red_red_violations, bst_violations;

	void validate(node_t *root, int _bh, int _th)
	{
		if (!root)
			return;
	
		node_t *left = root->left;
		node_t *right = root->right;
	
		total_nodes++;
		black_nodes += (IS_BLACK(root));
		red_nodes += (IS_RED(root));
		_th++;
		_bh += (IS_BLACK(root));
	
		/* BST violation? */
		if (left && left->key > root->key)
			bst_violations++;
		if (right && right->key <= root->key)
			bst_violations++;
	
		/* Red-Red violation? */
		if (IS_RED(root) && (IS_RED(left) || IS_RED(right)))
			red_red_violations++;
	
		/* We found a path (a node with at least one sentinel child). */
		if (!left || !right) {
			total_paths++;
			if (bh == -1)
				bh = _bh;
			else if (_bh != bh)
				paths_with_bh_diff++;
	
			if (_th <= min_path_len) {
				min_path_len = _th;
				key_in_min_path = root->key;
			}
			if (_th >= max_path_len) {
				max_path_len = _th;
				key_in_max_path = root->key;
			}
		}
	
		/* Check subtrees. */
		if (left)  validate(left, _bh, _th);
		if (right) validate(right, _bh, _th);
	}

	inline int validate_helper()
	{
		int check_bh = 0, check_red_red = 0, check_bst = 0;
		int check_rbt = 0;
		bh = -1;
		paths_with_bh_diff = 0;
		total_paths = 0;
		min_path_len = 99999999;
		max_path_len = -1;
		total_nodes = black_nodes = red_nodes = 0;
		red_red_violations = 0;
		bst_violations = 0;
	
		validate(root, 0, 0);
	
		check_bh = (paths_with_bh_diff == 0);
		check_red_red = (red_red_violations == 0);
		check_bst = (bst_violations == 0);
		check_rbt = (check_bh && check_red_red && check_bst);
	
		printf("Validation:\n");
		printf("=======================\n");
		printf("  Valid Red-Black Tree: %s\n",
		       check_rbt ? "Yes [OK]" : "No [ERROR]");
		printf("  Black height: %d [%s]\n", bh,
		       check_bh ? "OK" : "ERROR");
		printf("  Red-Red Violation: %s\n",
		       check_red_red ? "No [OK]" : "Yes [ERROR]");
		printf("  BST Violation: %s\n",
		       check_bst ? "No [OK]" : "Yes [ERROR]");
		printf("  Tree size (Total / Black / Red): %8d / %8d / %8d\n",
		       total_nodes, black_nodes, red_nodes);
		printf("  Total paths: %d\n", total_paths);
		printf("  Min/max paths length: %d/%d\n", min_path_len, max_path_len);
		printf("  Key in min path: %d\n", key_in_min_path);
		printf("  Key in max path: %d\n", key_in_max_path);
		printf("\n");
	
		return check_rbt;
	}
};

#define BST_RBT_INT_TEMPL template<typename K, typename V>
#define BST_RBT_INT_FUNCT bst_rbt_int<K,V>

BST_RBT_INT_TEMPL
bool BST_RBT_INT_FUNCT::contains(const int tid, const K& key)
{
	return lookup_helper(key);
}

BST_RBT_INT_TEMPL
const std::pair<V,bool> BST_RBT_INT_FUNCT::find(const int tid, const K& key)
{
	int ret = lookup_helper(key);
	return std::pair<V,bool>(NULL, ret);
}

BST_RBT_INT_TEMPL
int BST_RBT_INT_FUNCT::rangeQuery(const int tid, const K& lo, const K& hi,
                      std::vector<std::pair<K,V>> kv_pairs)
{
	return 0;
}

BST_RBT_INT_TEMPL
const V BST_RBT_INT_FUNCT::insert(const int tid, const K& key, const V& val)
{
	return insertIfAbsent(tid, key, val);
}

BST_RBT_INT_TEMPL
const V BST_RBT_INT_FUNCT::insertIfAbsent(const int tid, const K& key, const V& val)
{
	return insert_helper(key, val);
}

BST_RBT_INT_TEMPL
const std::pair<V,bool> BST_RBT_INT_FUNCT::remove(const int tid, const K& key)
{
	const V ret = delete_helper(key);
	return std::pair<V,bool>(ret, ret != this->NO_VALUE);
}

BST_RBT_INT_TEMPL
bool BST_RBT_INT_FUNCT::validate()
{
	return validate_helper();
}
