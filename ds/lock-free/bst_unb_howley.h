/**
 * An internal unbalanced binary search tree.
 * Paper:
 *    A non-blocking internal binary search tree, Howley et. al, SPAA 2012
 **/

#pragma once

#include "../map_if.h"
#include "Log.h"

#define CAS_PTR(a,b,c) __sync_val_compare_and_swap(a,b,c)
#define CAS_U32(a,b,c) __sync_val_compare_and_swap(a,b,c)

//Encoded in the operation pointer
#define STATE_OP_NONE 0
#define STATE_OP_MARK 1
#define STATE_OP_CHILDCAS 2
#define STATE_OP_RELOCATE 3

//In the relocate_op struct
#define STATE_OP_ONGOING 0
#define STATE_OP_SUCCESSFUL 1
#define STATE_OP_FAILED 2

//States for the result of a search operation
#define FOUND 0x0
#define NOT_FOUND_L 0x1
#define NOT_FOUND_R 0x2
#define ABORT 0x3

template <typename K, typename V>
class bst_unb_howley : public Map<K,V> {
public:
	bst_unb_howley(const K _NO_KEY, const V _NO_VALUE, const int numProcesses)
	  : Map<K,V>(_NO_KEY, _NO_VALUE)
	{
		root = new node_t(0, NULL);
	}

	void initThread(const int tid) {
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
	char *name() { return "BST Unbalanced Howley"; }

	void print() { print_struct(); };
//	unsigned long long size() { return size_rec(root) - 2; };

private:
//> Helper Macros
#define GETFLAG(op)    ((uint64_t)(op) & 3)
#define FLAG(op, flag) ((((uint64_t)(op)) & 0xfffffffffffffffc) | (flag))
#define UNFLAG(op)     (((uint64_t)(op)) & 0xfffffffffffffffc)
#define ISNULL(node)   (((node) == NULL) || (((uint64_t)node) & 1))
#define SETNULL(node)  ((((uint64_t)node) & 0xfffffffffffffffe) | 1)

	union operation_t;

	struct node_t {
		K key; 
	    V value;
		operation_t *op;
		node_t *left,
		       *right;
		//char padding[CACHE_LINE_SIZE - 40];

		node_t(const K& key, const V& value) {
			this->key = key;
			this->value = value;
			this->op = NULL;
			this->left = this->right = NULL;
		};
	};

	struct child_cas_op_t {
		bool is_left;
		node_t *expected,
		       *update;
		//char padding[40]; 
	};

	struct relocate_op_t {
		int state; // initialize to ONGOING every time a relocate operation is created
		node_t *dest;
		operation_t *dest_op;
		K remove_key;
		V remove_value;
		K replace_key;
		V replace_value;
		//char padding[32]; 
	};

	union operation_t {
		child_cas_op_t child_cas_op;
		relocate_op_t relocate_op;
    	char padding[64];
	};

	node_t *root;

private:

	void help_child_cas(operation_t *op, node_t *dest)
	{
		node_t **address = NULL;
		if (op->child_cas_op.is_left) address = (node_t **) &(dest->left);
		else                          address = (node_t **) &(dest->right);
		void *dummy0 = CAS_PTR(address, op->child_cas_op.expected, op->child_cas_op.update);
		void *dummy1 = CAS_PTR(&(dest->op), FLAG(op, STATE_OP_CHILDCAS), FLAG(op, STATE_OP_NONE));
	}

	bool help_relocate(operation_t *op, node_t *pred, operation_t *pred_op, node_t *curr)
	{
		int seen_state = op->relocate_op.state;
		if (seen_state == STATE_OP_ONGOING) {
			// VCAS in original implementation
			operation_t *seen_op = CAS_PTR(&(op->relocate_op.dest->op), op->relocate_op.dest_op, FLAG(op, STATE_OP_RELOCATE));
			if ((seen_op == op->relocate_op.dest_op) || (seen_op == (operation_t *)FLAG(op, STATE_OP_RELOCATE))){
				CAS_U32(&(op->relocate_op.state), STATE_OP_ONGOING, STATE_OP_SUCCESSFUL);
				seen_state = STATE_OP_SUCCESSFUL;
			} else {
				// VCAS in original implementation
				seen_state = CAS_U32(&(op->relocate_op.state), STATE_OP_ONGOING, STATE_OP_FAILED);
			}
		}
	
		if (seen_state == STATE_OP_SUCCESSFUL) {
			CAS_PTR(&(op->relocate_op.dest->key), op->relocate_op.remove_key, op->relocate_op.replace_key);
			CAS_PTR(&(op->relocate_op.dest->value), op->relocate_op.remove_value, op->relocate_op.replace_value);
			void *dummy2 = CAS_PTR(&(op->relocate_op.dest->op), FLAG(op, STATE_OP_RELOCATE), FLAG(op, STATE_OP_NONE));
		}
	
		char result = (seen_state == STATE_OP_SUCCESSFUL);
		if (op->relocate_op.dest == curr)
			return result;
	
		void *dummy = CAS_PTR(&(curr->op), FLAG(op, STATE_OP_RELOCATE), FLAG(op, result ? STATE_OP_MARK : STATE_OP_NONE));
		if (result) {
			if (op->relocate_op.dest == pred)
				pred_op = (operation_t *)FLAG(op, STATE_OP_NONE);
			help_marked(pred, pred_op, curr);
		}
		return result;
	}
	
	void help_marked(node_t *pred, operation_t *pred_op, node_t *curr)
	{
		node_t *new_ref;
		if (ISNULL((node_t*) curr->left)) {
			if (ISNULL((node_t*) curr->right)) new_ref = (node_t*)SETNULL(curr);
			else                               new_ref = (node_t*) curr->right;
		} else {
			new_ref = (node_t*) curr->left;
		}
		operation_t *cas_op = new operation_t();
		cas_op->child_cas_op.is_left = (curr == pred->left);
		cas_op->child_cas_op.expected = curr;
		cas_op->child_cas_op.update = new_ref;
	
		if (CAS_PTR(&(pred->op), pred_op, FLAG(cas_op, STATE_OP_CHILDCAS)) == pred_op)
			help_child_cas(cas_op, pred);
	}
	
	void help(node_t *pred, operation_t *pred_op,
	          node_t *curr, operation_t *curr_op)
	{
		if (GETFLAG(curr_op) == STATE_OP_CHILDCAS)
			help_child_cas((operation_t*)UNFLAG(curr_op), curr);
		else if (GETFLAG(curr_op) == STATE_OP_RELOCATE)
			help_relocate((operation_t*)UNFLAG(curr_op), pred, pred_op, curr);
		else if (GETFLAG(curr_op) == STATE_OP_MARK)
			help_marked(pred, pred_op, curr);
	}


	int search(const K& k, node_t **pred, operation_t **pred_op,
	           node_t **curr, operation_t **curr_op, node_t *aux_root)
	{
		int result;
		K curr_key;
		node_t *next, *last_right;
		operation_t *last_right_op;
	
	RETRY_LABEL:
		result = NOT_FOUND_R;
		*curr = aux_root;
		*curr_op = (*curr)->op;
	
		if(GETFLAG(*curr_op) != STATE_OP_NONE){
			if (aux_root == root){
				help_child_cas((operation_t*)UNFLAG(*curr_op), *curr);
				goto RETRY_LABEL;
			} else {
				return ABORT;
			}
		}
	
		next = (*curr)->right;
		last_right = *curr;
		last_right_op = *curr_op;
	
		while (!ISNULL(next)){
			*pred = *curr;
			*pred_op = *curr_op;
			*curr = next;
			*curr_op = (*curr)->op;
	
			if(GETFLAG(*curr_op) != STATE_OP_NONE){
				help(*pred, *pred_op, *curr, *curr_op);
				goto RETRY_LABEL;
			}
	
			curr_key = (*curr)->key;
			if(k < curr_key){
				result = NOT_FOUND_L;
				next = (node_t*) (*curr)->left;
			} else if (k > curr_key) {
				result = NOT_FOUND_R;
				next = (node_t*) (*curr)->right;
				last_right = *curr;
				last_right_op = *curr_op;
			} else{
				result = FOUND;
				break;
			}
		}
		
		if (result != FOUND && last_right_op != last_right->op)
			goto RETRY_LABEL;
	
		if ((*curr)->op != *curr_op)
			goto RETRY_LABEL;
	
		return result;
	}
	
	int lookup_helper(const K& k)
	{
		node_t *pred, *curr;
		operation_t *pred_op, *curr_op;
	
	    int ret = search(k, &pred, &pred_op, &curr, &curr_op, root);
		return (ret == FOUND);
	}

	int do_insert(const K& k, const V& v, int result, node_t **new_node,
	              node_t *old, node_t *curr, operation_t *curr_op)
	{
		operation_t *cas_op;
		bool is_left;
	
		if (*new_node == NULL)
			*new_node = new node_t(k, v);
	
		is_left = (result == NOT_FOUND_L);
		if (is_left) old = curr->left;
		else         old = curr->right;
	
		cas_op = new operation_t();
		cas_op->child_cas_op.is_left = is_left;
		cas_op->child_cas_op.expected = old;
		cas_op->child_cas_op.update = *new_node;
	
		if (CAS_PTR(&curr->op, curr_op, FLAG(cas_op, STATE_OP_CHILDCAS)) == curr_op) {
			help_child_cas(cas_op, curr);
			return 1;
		}
		return 0;
	}
	
	const V insert_helper(const K& k, const V& v)
	{
		node_t *pred, *curr, *new_node = NULL, *old;
		operation_t *pred_op, *curr_op;
		int result = 0;
	
		while(1) {
			result = search(k, &pred, &pred_op, &curr, &curr_op, root);
			if (result == FOUND)
				return curr->value;
			if (do_insert(k, v, result, &new_node, old, curr, curr_op))
				return this->NO_VALUE;
		}
	}

	int do_delete(const K& k, node_t *curr, node_t *pred,
	              operation_t *curr_op, operation_t *pred_op,
	              operation_t **reloc_op)
	{
		node_t *replace;
		operation_t *replace_op;
		int res;
	
		//> Node has less than two children
		if (ISNULL(curr->right) || ISNULL(curr->left)) {
			if (CAS_PTR(&(curr->op), curr_op, FLAG(curr_op, STATE_OP_MARK)) == curr_op) {
				help_marked(pred, pred_op, curr);
				return 1;
			}
		//> Node has two children
		} else {
			res = search(k, &pred, &pred_op, &replace, &replace_op, curr);
			if (res == ABORT || curr->op != curr_op)
				return 0;
	            
			if (*reloc_op == NULL) *reloc_op = new operation_t(); 
			(*reloc_op)->relocate_op.state = STATE_OP_ONGOING;
			(*reloc_op)->relocate_op.dest = curr;
			(*reloc_op)->relocate_op.dest_op = curr_op;
			(*reloc_op)->relocate_op.remove_key = k;
			(*reloc_op)->relocate_op.remove_value = (V)res;
			(*reloc_op)->relocate_op.replace_key = replace->key;
			(*reloc_op)->relocate_op.replace_value = replace->value;
	
			if (CAS_PTR(&(replace->op), replace_op, FLAG(*reloc_op, STATE_OP_RELOCATE)) == replace_op)
				if (help_relocate(*reloc_op, pred, pred_op, replace))
					return 1;
		}
		return 0;
	}
	
	const V delete_helper(const K& k)
	{
		node_t *pred, *curr;
		operation_t *pred_op, *curr_op, *reloc_op = NULL;
	    int res;
	
		while(1) {
	        res = search(k, &pred, &pred_op, &curr, &curr_op, root);
			if (res != FOUND)
				return this->NO_VALUE;
			const V del_val = curr->value;
			if (do_delete(k,curr, pred, curr_op, pred_op, &reloc_op))
				return del_val;
		}
	}

//static int bst_update(int k, void *v, node_t *root, tdata_t *tdata)
//{
//	node_t *pred, *curr, *new_node = NULL, *old;
//	operation_t *pred_op, *curr_op, *reloc_op = NULL;
//	int res = 0;
//	int op_is_insert = -1;
//
//	while (1) {
//		res = bst_find(k, &pred, &pred_op, &curr, &curr_op, root, root, tdata);
//		if (op_is_insert == -1) {
//			if (res == FOUND) op_is_insert = 0;
//			else              op_is_insert = 1;
//		}
//
//		if (op_is_insert) {
//			if (res == FOUND)
//				return 0;
//			if (do_bst_add(k, v, res, root, &new_node, old, curr, curr_op))
//				return 1;
//			tdata->retries[1]++;
//		} else {
//			if (res != FOUND)
//				return 2;
//			if (do_bst_remove(k, root, curr, pred, curr_op, pred_op, &reloc_op, tdata))
//				return 3;
//			tdata->retries[2]++;
//		}
//	}
//	return 0;
//}

	int total_paths, total_nodes, bst_violations;
	int min_path_len, max_path_len;
	void validate_rec(node_t *root, int _th)
	{
		if (ISNULL(root))
			return;
	
		node_t *left = root->left;
		node_t *right = root->right;
	
		total_nodes++;
		_th++;
	
		/* BST violation? */
		if (!ISNULL(left) && left->key >= root->key)
			bst_violations++;
		if (!ISNULL(right) && right->key < root->key)
			bst_violations++;
	
		/* We found a path (a node with at least one sentinel child). */
		if (ISNULL(left) && ISNULL(right)) {
			total_paths++;
	
			if (_th <= min_path_len)
				min_path_len = _th;
			if (_th >= max_path_len)
				max_path_len = _th;
		}
	
		/* Check subtrees. */
		validate_rec(left, _th);
		validate_rec(right, _th);
	}
	
	inline int validate_helper()
	{
		int check_bst = 0;
		total_paths = 0;
		min_path_len = 99999999;
		max_path_len = -1;
		total_nodes = 0;
		bst_violations = 0;
	
		validate_rec(root->right, 0);
	
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
		int i;
	
		if (!ISNULL(root))
			print_rec(root->right, level + 1);
	
		for (i = 0; i < level; i++)
			std::cout << "|--";
	
		if (ISNULL(root)) {
			std::cout << "NULL\n";
			return;
		}
	
		std::cout << root->key << "\n";
	
		print_rec(root->left, level + 1);
	}
	void print_struct()
	{
		if (ISNULL(root)) printf("[empty]");
		else print_rec(root, 0);
		printf("\n");
	}
	/******************************************************************************/
};

#define BST_UNB_HOWLEY_TEMPL template<typename K, typename V>
#define BST_UNB_HOWLEY_FUNCT bst_unb_howley<K,V>

BST_UNB_HOWLEY_TEMPL
bool BST_UNB_HOWLEY_FUNCT::contains(const int tid, const K& key)
{
	return lookup_helper(key);
}

BST_UNB_HOWLEY_TEMPL
const std::pair<V,bool> BST_UNB_HOWLEY_FUNCT::find(const int tid, const K& key)
{
	int ret = lookup_helper(key);
	return std::pair<V,bool>(NULL, ret);
}

BST_UNB_HOWLEY_TEMPL
int BST_UNB_HOWLEY_FUNCT::rangeQuery(const int tid, const K& lo, const K& hi,
                      std::vector<std::pair<K,V>> kv_pairs)
{
	return 0;
}

BST_UNB_HOWLEY_TEMPL
const V BST_UNB_HOWLEY_FUNCT::insert(const int tid, const K& key, const V& val)
{
	return insertIfAbsent(tid, key, val);
}

BST_UNB_HOWLEY_TEMPL
const V BST_UNB_HOWLEY_FUNCT::insertIfAbsent(const int tid, const K& key, const V& val)
{
	return insert_helper(key, val);
}

BST_UNB_HOWLEY_TEMPL
const std::pair<V,bool> BST_UNB_HOWLEY_FUNCT::remove(const int tid, const K& key)
{
	const V ret = delete_helper(key);
	return std::pair<V,bool>(ret, ret != this->NO_VALUE);
}

BST_UNB_HOWLEY_TEMPL
bool BST_UNB_HOWLEY_FUNCT::validate()
{
	return validate_helper();
}
