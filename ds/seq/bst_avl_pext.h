/**
 * A partially external AVL tree.
 **/

#pragma once

#include "../rcu-htm/ht.h"
#include "../map_if.h"
#include "Log.h"

#define MAX_HEIGHT 50

#define IS_MARKED(n) (n->marked)
#define MARK_NODE(n) do { \
	(n)->marked = true; \
} while (0)
#define UNMARK_NODE(n) do { \
	(n)->marked = false; \
} while (0)

template <typename K, typename V>
class bst_avl_pext : public Map<K,V> {
public:
	bst_avl_pext(const K _NO_KEY, const V _NO_VALUE, const int numProcesses)
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
	char *name() { return "BST AVL Partially-External"; }

	void print() { print_helper(); };
//	unsigned long long size() { return size_rec(root); };

private:

	struct node_t {
		K key;
		V value;

		int height;
		bool marked;
	
		node_t *left,
		       *right;

		node_t(const K& key, const V& value) {
			this->key = key;
			this->value = value;
			this->height = 0;
			UNMARK_NODE(this);
			this->left = this->right = NULL;
		}
	};

	node_t *root;

private:
	node_t *node_new_copy(node_t *src) {
		node_t *n = new node_t(src->key, src->value);
		n->value = src->value;
		n->height = src->height;
		n->left = src->left;
		n->right = src->right;
		return n;
	}

	inline int node_height(node_t *n)
	{
		if (!n) return -1;
		else    return n->height;
	}
	
	inline int node_balance(node_t *n)
	{
		if (!n) return 0;
		return node_height(n->left) - node_height(n->right);
	}

	inline node_t *rotate_right(node_t *node)
	{
		assert(node != NULL && node->left != NULL);
	
		node_t *node_left = node->left;
	
		node->left = node->left->right;
		node_left->right = node;
	
		node->height = MAX(node_height(node->left), node_height(node->right)) + 1;
		node_left->height = MAX(node_height(node_left->left), node_height(node_left->right)) + 1;
		return node_left;
	}
	inline node_t *rotate_left(node_t *node)
	{
		assert(node != NULL && node->right != NULL);
	
		node_t *node_right = node->right;
	
		node->right = node->right->left;
		node_right->left = node;
	
		node->height = MAX(node_height(node->left), node_height(node->right)) + 1;
		node_right->height = MAX(node_height(node_right->left), node_height(node_right->right)) + 1;
		return node_right;
	}

	/**
	 * Traverses the tree `avl` as dictated by `key`.
	 * When returning, `leaf` is either NULL (key not found) or the leaf that
	 * contains `key`. `parent` is either leaf's parent (if `leaf` != NULL) or
	 * the node that will be the parent of the inserted node.
	 * In the case of an empty tree both `parent` and `leaf` are NULL.
	 **/
	inline void traverse(const K& key, node_t **parent,node_t **leaf)
	{
		*parent = NULL;
		*leaf = root;
	
		while (*leaf) {
			if ((*leaf)->key == key) return;
	
			*parent = *leaf;
			*leaf = (key < (*leaf)->key) ? (*leaf)->left : (*leaf)->right;
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
	
			if (leaf->key == key) return;
	
			parent = leaf;
			leaf = (key < leaf->key) ? leaf->left : leaf->right;
		}
	}
	
	int lookup_helper(const K& key)
	{
		node_t *parent, *leaf;
	
		traverse(key, &parent, &leaf);
		return (leaf != NULL && !IS_MARKED(leaf));
	}

	inline void insert_fixup(const K& key, node_t *node_stack[MAX_HEIGHT], int top)
	{
		node_t *curr, *parent;
	
		while (top >= 0) {
			curr = node_stack[top--];
	
			parent = NULL;
			if (top >= 0) parent = node_stack[top];
	
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
		}
	}

	inline int do_insert(const K& key, const V& value,
	                     node_t *node_stack[MAX_HEIGHT], int stack_top)
	{
		// Empty tree case
		if (stack_top < 0) {
			root = new node_t(key, value);
			return 1;
		}
	
		node_t *place = node_stack[stack_top];
	
		// Key already in the tree.
		if (place->key == key) {
			if (!IS_MARKED(place)) {
				return 0;
			} else {
				UNMARK_NODE(place);
				return 1;
			}
		}
	
		if (key < place->key) place->left = new node_t(key, value);
		else                  place->right = new node_t(key, value);
	
		return 1;
	}

	const V insert_helper(const K& key, const V& value)
	{
		node_t *node_stack[MAX_HEIGHT];
		int stack_top;

		traverse_with_stack(key, node_stack, &stack_top);
		int ret = do_insert(key, value, node_stack, stack_top);
		if (!ret) return node_stack[stack_top]->value;
		insert_fixup(key, node_stack, stack_top);
		return this->NO_VALUE;
	}

	void delete_fixup(const K& key, node_t *node_stack[MAX_HEIGHT], int top)
	{
		node_t *curr, *parent;
	
		while (top >= 0) {
			curr = node_stack[top--];
	
			parent = NULL;
			if (top >= 0) parent = node_stack[top];
	
			int balance = node_balance(curr);
			if (balance == 2) {
				int balance2 = node_balance(curr->left);
	
				if (balance2 == 0 || balance2 == 1) { // LEFT-LEFT case
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
	
				continue;
			} else if (balance == -2) {
				int balance2 = node_balance(curr->right);
	
				if (balance2 == 0 || balance2 == -1) { // RIGHT-RIGHT case
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
	
				continue;
			}
	
			/* Update the height of current node. */
			int height_saved = node_height(curr);
			int height_new = MAX(node_height(curr->left), node_height(curr->right)) + 1;
			curr->height = height_new;
			if (height_saved == height_new)
				break;
		}
	}

	void do_delete(const K& key, node_t *node_stack[MAX_HEIGHT], int stack_top)
	{
		node_t *place = node_stack[stack_top];
		node_t *parent = (stack_top-1 >= 0) ? node_stack[stack_top-1] : NULL;

		if (!place->left) {
			if (!parent) root = place->right;
			else if (parent->left == place) parent->left = place->right;
			else if (parent->right == place) parent->right = place->right;
		} else if (!place->right) {
			if (!parent) root = place->left;
			else if (parent->left == place) parent->left = place->left;
			else if (parent->right == place) parent->right = place->left;
		} else { // place has two children.
			MARK_NODE(place);
			return;
		}
	
		stack_top--;
		delete_fixup(key, node_stack, stack_top);
	}

	const V delete_helper(const K& key)
	{
		node_t *node_stack[MAX_HEIGHT];
		int stack_top;
	
		traverse_with_stack(key, node_stack, &stack_top);
	
		// Key not in the tree
		if (stack_top < 0 || node_stack[stack_top]->key != key
		                  || IS_MARKED(node_stack[stack_top]))
			return this->NO_VALUE;
	
		const V ret = node_stack[stack_top]->value;
		do_delete(key, node_stack, stack_top);
		return ret;
	}

private:
	int total_paths, total_nodes, bst_violations, avl_violations;
	int min_path_len, max_path_len;
	int marked_nodes;
	void validate_rec(node_t *root, int _th)
	{
		if (!root)
			return;
	
		if (IS_MARKED(root))
			marked_nodes++;
	
		node_t *left = root->left;
		node_t *right = root->right;
	
		total_nodes++;
		_th++;
	
		/* BST violation? */
		if (left && left->key >= root->key)
			bst_violations++;
		if (right && right->key <= root->key)
			bst_violations++;
	
		/* AVL violation? */
		int balance = node_balance(root);
		if (balance < -1 || balance > 1)
			avl_violations++;
	
		/* We found a path (a node with at least one NULL child). */
		if (!left || !right) {
			total_paths++;
	
			if (_th <= min_path_len)
				min_path_len = _th;
			if (_th >= max_path_len)
				max_path_len = _th;
		}
	
		/* Check subtrees. */
		if (left)  validate_rec(left, _th);
		if (right) validate_rec(right, _th);
	}
	
	inline int validate_helper()
	{
		int check_bst = 0, check_avl = 0;
		total_paths = 0;
		min_path_len = 99999999;
		max_path_len = -1;
		total_nodes = 0;
		marked_nodes = 0;
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
		printf("  Tree size (UnMarked / Marked): %8d / %8d\n", total_nodes - marked_nodes, marked_nodes);
		printf("  Total paths: %d\n", total_paths);
		printf("  Min/max paths length: %d/%d\n", min_path_len, max_path_len);
		printf("\n");
	
		return check_bst && check_avl;
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
	
		printf("%d [%d]\n", root->key, root->height);
	
		print_rec(root->left, level + 1);
	}
	
	void print_helper()
	{
		if (root == NULL) printf("[empty]");
		else              print_rec(root, 0);
		printf("\n");
	}
	/******************************************************************************/


public:
	/**
	 * RCU-HTM adapting methods.
	 **/
	const V traverse_with_stack(const K& key, void **stack,
	                            int *stack_indexes, int *stack_top)
	{
		node_t *parent, *leaf;
		node_t **node_stack = (node_t **)stack;
	
		parent = NULL;
		leaf = root;
		*stack_top = -1;
	
		while (leaf) {
			node_stack[++(*stack_top)] = leaf;
			stack_indexes[*stack_top] = (key <= leaf->key) ? 0 : 1;

			if (leaf->key == key) break;
			parent = leaf;
			leaf = (key <= leaf->key) ? leaf->left : leaf->right;
		}

		if (*stack_top >= 0 && !IS_MARKED(node_stack[*stack_top])
		                    && node_stack[*stack_top]->key == key)
			return node_stack[*stack_top]->value;
		else
			return this->NO_VALUE;
	}
	void install_copy(void *connpoint, void *privcopy,
	                  int *node_stack_indexes, int connpoint_stack_index)
	{
		node_t *connection_point = (node_t *)connpoint;
		node_t *tree_copy_root = (node_t *)privcopy;
		if (!connection_point) {
			root = tree_copy_root;
		} else {
			int index = node_stack_indexes[connpoint_stack_index];
			if (index == 0) connection_point->left = tree_copy_root;
			else            connection_point->right = tree_copy_root;
		}
	}
	void validate_copy(void **stack, int *node_stack_indexes,
	                   int stack_top)
	{
		node_t **node_stack = (node_t **)stack;
		node_t *n1, *n2;

		if (stack_top < 0 && root != NULL)
			TX_ABORT(ABORT_VALIDATION_FAILURE);
		if (stack_top >= 0 && root != node_stack[0])
			TX_ABORT(ABORT_VALIDATION_FAILURE);
		if (stack_top >= 0 && node_stack_indexes[stack_top] == 0 && node_stack[stack_top]->left != NULL)
			TX_ABORT(ABORT_VALIDATION_FAILURE);
		if (stack_top >= 0 && node_stack_indexes[stack_top] == 1 && node_stack[stack_top]->right != NULL)
			TX_ABORT(ABORT_VALIDATION_FAILURE);
		for (int i=0; i < stack_top; i++) {
			n1 = node_stack[i];
			int index = node_stack_indexes[i];
			n2 = (node_t *)((index == 0) ? n1->left : n1->right);
			if (n2 != node_stack[i+1])
					TX_ABORT(ABORT_VALIDATION_FAILURE);
		}
	
		for (int i=0; i < HT_LEN; i++) {
			for (int j=0; j < tdata->ht->bucket_next_index[i]; j+=2) {
				node_t **np = (node_t **)tdata->ht->entries[i][j];
				node_t  *n  = (node_t *)tdata->ht->entries[i][j+1];
				if (*np != n)
					TX_ABORT(ABORT_VALIDATION_FAILURE);
			}
		}

	}
	void *insert_with_copy(const K& key, const V& value, void **stack,
	                       int *stack_indexes, int stack_top, void **privcopy,
	                       int *connpoint_stack_index)
	{
		node_t *tree_copy_root, *connection_point;
		node_t **node_stack = (node_t **)stack;
	
		//> Empty tree case
		if (stack_top < 0) {
			*connpoint_stack_index = stack_top;
			connection_point = stack_top >= 0 ? node_stack[stack_top--] : NULL;
			*privcopy = (void *)new node_t(key, value);
			return (void *)connection_point;
		}
	
		//> Marked node case
		node_t *n = node_stack[stack_top];
		if (n->key == key && IS_MARKED(n)) {
			*privcopy = node_new_copy(n);
			ht_insert(tdata->ht, &n->left,  ((node_t *)(*privcopy))->left);
			ht_insert(tdata->ht, &n->right, ((node_t *)(*privcopy))->right);
	
			UNMARK_NODE((node_t *)*privcopy);
			*connpoint_stack_index = stack_top - 1;
			return ((stack_top-1) >= 0 ? node_stack[stack_top-1] : NULL);
		}
	
		//> Start the tree copying with the new node.
		tree_copy_root = new node_t(key, value);
		*connpoint_stack_index = stack_top;
		connection_point = stack_top >= 0 ? node_stack[stack_top--] : NULL;
	
		while (stack_top >= -1) {
			//> If we've reached and passed root return.
			if (!connection_point)
				break;
	
			//> If no height change occurs we can break.
			if (tree_copy_root->height + 1 <= connection_point->height)
				break;
	
			//> Copy the current node and link it to the local copy.
			node_t *curr_cp = node_new_copy(connection_point);
			ht_insert(tdata->ht, &connection_point->left, curr_cp->left);
			ht_insert(tdata->ht, &connection_point->right, curr_cp->right);
	
			curr_cp->height = tree_copy_root->height + 1;
			if (key < curr_cp->key) curr_cp->left = tree_copy_root;
			else                    curr_cp->right = tree_copy_root;
			tree_copy_root = curr_cp;
	
			// Move one level up
			*connpoint_stack_index = stack_top;
			connection_point = stack_top >= 0 ? node_stack[stack_top--] : NULL;
	
			// Get current node's balance
			node_t *sibling;
			int curr_balance;
			if (key < curr_cp->key) {
				sibling = curr_cp->right;
				curr_balance = node_height(curr_cp->left) - node_height(sibling);
			} else {
				sibling = curr_cp->left;
				curr_balance = node_height(sibling) - node_height(curr_cp->right);
			}
	
			if (curr_balance == 2) {
				int balance2 = node_balance(tree_copy_root->left);
	
				if (balance2 == 1) { // LEFT-LEFT case
					tree_copy_root = rotate_right(tree_copy_root);
				} else if (balance2 == -1) { // LEFT-RIGHT case
					tree_copy_root->left = rotate_left(tree_copy_root->left);
					tree_copy_root = rotate_right(tree_copy_root);
				} else {
					assert(0);
				}
	
				break;
			} else if (curr_balance == -2) {
				int balance2 = node_balance(tree_copy_root->right);
	
				if (balance2 == -1) { // RIGHT-RIGHT case
					tree_copy_root = rotate_left(tree_copy_root);
				} else if (balance2 == 1) { // RIGHT-LEFT case
					tree_copy_root->right = rotate_right(tree_copy_root->right);
					tree_copy_root = rotate_left(tree_copy_root);
				} else {
					assert(0);
				}
	
				break;
			}
		}
	
		*privcopy = (void *)tree_copy_root;
		return (void *)connection_point;

	}
	void *delete_with_copy(const K& key, void **stack, int *unused,
	                       int *_stack_top, void **privcopy,
	                       int *connpoint_stack_index)
	{
		node_t *tree_copy_root, *connection_point;
		node_t **node_stack = (node_t **)stack;
		int stack_top = *_stack_top;
		int to_be_deleted_stack_index = stack_top;
		node_t *l, *r;
	
		node_t *to_be_deleted = node_stack[stack_top];
		l = to_be_deleted->left; r = to_be_deleted->right;
		ht_insert(tdata->ht, &to_be_deleted->left, l);
		ht_insert(tdata->ht, &to_be_deleted->right, r);
	
		//> Mark node as removed
		if (l != NULL && r != NULL) {
			*privcopy = (void *)node_new_copy(to_be_deleted);
			MARK_NODE((node_t *)*privcopy);
			*connpoint_stack_index = stack_top - 1;
			return ((stack_top-1) >= 0 ? node_stack[stack_top-1] : NULL);
		}
	
		tree_copy_root = (l != NULL) ? l : r;
		stack_top--;
		*connpoint_stack_index = stack_top;
		connection_point = stack_top >= 0 ? node_stack[stack_top--] : NULL;
	
		while (stack_top >= -1) {
			// If we've reached and passed root return.
			if (!connection_point)
				break;
	
			node_t *sibling;
			int curr_balance;
			if (key < connection_point->key) {
				sibling = connection_point->right;
				ht_insert(tdata->ht, &connection_point->right, sibling);
				curr_balance = node_height(tree_copy_root) - node_height(sibling);
			} else {
				sibling = connection_point->left;
				ht_insert(tdata->ht, &connection_point->left, sibling);
				curr_balance = node_height(sibling) - node_height(tree_copy_root);
			}
	
			// Check if rotation(s) is(are) necessary.
			if (curr_balance == 2) {
				node_t *curr_cp = node_new_copy(connection_point);
				ht_insert(tdata->ht, &connection_point->left, curr_cp->left);
				ht_insert(tdata->ht, &connection_point->right, curr_cp->right);
				curr_cp->left = sibling;
	
				if (key < curr_cp->key) curr_cp->left = tree_copy_root;
				else                    curr_cp->right = tree_copy_root;
				tree_copy_root = curr_cp;
				curr_cp = node_new_copy(tree_copy_root->left);
				ht_insert(tdata->ht, &tree_copy_root->left->left, curr_cp->left);
				ht_insert(tdata->ht, &tree_copy_root->left->right, curr_cp->right);
				tree_copy_root->left = curr_cp;
	
				int balance2 = node_balance(tree_copy_root->left);
	
				if (balance2 == 0 || balance2 == 1) { // LEFT-LEFT case
					tree_copy_root = rotate_right(tree_copy_root);
				} else if (balance2 == -1) { // LEFT-RIGHT case
					curr_cp = node_new_copy(tree_copy_root->left->right);
					ht_insert(tdata->ht, &tree_copy_root->left->right->left, curr_cp->left);
					ht_insert(tdata->ht, &tree_copy_root->left->right->right, curr_cp->right);
					tree_copy_root->left->right = curr_cp;
	
					tree_copy_root->left = rotate_left(tree_copy_root->left);
					tree_copy_root = rotate_right(tree_copy_root);
				} else {
					assert(0);
				}
	
				// Move one level up
				*connpoint_stack_index = stack_top;
				connection_point = stack_top >= 0 ? node_stack[stack_top--] : NULL;
				continue;
			} else if (curr_balance == -2) {
				node_t *curr_cp = node_new_copy(connection_point);
				ht_insert(tdata->ht, &connection_point->left, curr_cp->left);
				ht_insert(tdata->ht, &connection_point->right, curr_cp->right);
				curr_cp->right = sibling;
	
				if (key < curr_cp->key) curr_cp->left = tree_copy_root;
				else                    curr_cp->right = tree_copy_root;
				tree_copy_root = curr_cp;
				curr_cp = node_new_copy(tree_copy_root->right);
				ht_insert(tdata->ht, &tree_copy_root->right->left, curr_cp->left);
				ht_insert(tdata->ht, &tree_copy_root->right->right, curr_cp->right);
				tree_copy_root->right = curr_cp;
	
				int balance2 = node_balance(tree_copy_root->right);
	
				if (balance2 == 0 || balance2 == -1) { // RIGHT-RIGHT case
					tree_copy_root = rotate_left(tree_copy_root);
				} else if (balance2 == 1) { // RIGHT-LEFT case
					curr_cp = node_new_copy(tree_copy_root->right->left);
					ht_insert(tdata->ht, &tree_copy_root->right->left->left, curr_cp->left);
					ht_insert(tdata->ht, &tree_copy_root->right->left->right, curr_cp->right);
					tree_copy_root->right->left = curr_cp;
					tree_copy_root->right = rotate_right(tree_copy_root->right);
					tree_copy_root = rotate_left(tree_copy_root);
				} else {
					assert(0);
				}
	
				// Move one level up
				*connpoint_stack_index = stack_top;
				connection_point = stack_top >= 0 ? node_stack[stack_top--] : NULL;
				continue;
			}
	
			// Check whether current node's height is to change.
			int old_height = connection_point->height;
			int new_height = MAX(node_height(tree_copy_root), node_height(sibling)) + 1;
			if (old_height == new_height)
				break;
	
			// Copy the current node and link it to the local copy.
			node_t *curr_cp = node_new_copy(connection_point);
			if (key < curr_cp->key) curr_cp->right = sibling;
			else                    curr_cp->left = sibling;
	
			ht_insert(tdata->ht, &connection_point->left, curr_cp->left);
			ht_insert(tdata->ht, &connection_point->right, curr_cp->right);
	
			// Change the height of current node's copy + the key if needed.
			curr_cp->height = new_height;
			if (key < curr_cp->key) curr_cp->left = tree_copy_root;
			else                    curr_cp->right = tree_copy_root;
			tree_copy_root = curr_cp;
	
			// Move one level up
			*connpoint_stack_index = stack_top;
			connection_point = stack_top >= 0 ? node_stack[stack_top--] : NULL;
	
		}
	
		*privcopy = (void *)tree_copy_root;
		return (void *)connection_point;
	}
};

#define BST_AVL_PEXT_TEMPL template<typename K, typename V>
#define BST_AVL_PEXT_FUNCT bst_avl_pext<K,V>

BST_AVL_PEXT_TEMPL
bool BST_AVL_PEXT_FUNCT::contains(const int tid, const K& key)
{
	return lookup_helper(key);
}

BST_AVL_PEXT_TEMPL
const std::pair<V,bool> BST_AVL_PEXT_FUNCT::find(const int tid, const K& key)
{
	int ret = lookup_helper(key);
	return std::pair<V,bool>(NULL, ret);
}

BST_AVL_PEXT_TEMPL
int BST_AVL_PEXT_FUNCT::rangeQuery(const int tid, const K& lo, const K& hi,
                      std::vector<std::pair<K,V>> kv_pairs)
{
	return 0;
}

BST_AVL_PEXT_TEMPL
const V BST_AVL_PEXT_FUNCT::insert(const int tid, const K& key, const V& val)
{
	return insertIfAbsent(tid, key, val);
}

BST_AVL_PEXT_TEMPL
const V BST_AVL_PEXT_FUNCT::insertIfAbsent(const int tid, const K& key, const V& val)
{
	return insert_helper(key, val);
}

BST_AVL_PEXT_TEMPL
const std::pair<V,bool> BST_AVL_PEXT_FUNCT::remove(const int tid, const K& key)
{
	const V ret = delete_helper(key);
	return std::pair<V,bool>(ret, ret != this->NO_VALUE);
}

BST_AVL_PEXT_TEMPL
bool BST_AVL_PEXT_FUNCT::validate()
{
	return validate_helper();
}
