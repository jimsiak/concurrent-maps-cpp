/**
 * An internal unbalanced binary search tree.
 * Paper:
 *    Non-blocking Binary Search Trees, Ellen et. al, PODC 2010
 **/

#pragma once

#include "../map_if.h"
#include "Log.h"

#define MEM_BARRIER __sync_synchronize()
#define CAS_PTR(a,b,c) __sync_val_compare_and_swap(a,b,c)

// The states a node can have: we avoid an enum to better control the size of
//                             the data structures
#define STATE_CLEAN 0
#define STATE_DFLAG 1
#define STATE_IFLAG 2
#define STATE_MARK 3

#define ELLEN_MAX_KEY (this->INF_KEY)
#define ELLEN_INF2 (ELLEN_MAX_KEY)
#define ELLEN_INF1 (ELLEN_MAX_KEY - 1)

static __thread void *last_result_threadlocal; // FIXME is this OK to be here?

template <typename K, typename V>
class bst_unb_ellen : public Map<K,V> {
public:
	bst_unb_ellen(const K _NO_KEY, const V _NO_VALUE, const int numProcesses)
	  : Map<K,V>(_NO_KEY, _NO_VALUE)
	{
		root = new node_t(ELLEN_INF2, 0, false);
		root->left = new node_t(ELLEN_INF1, 0, true);
		root->right = new node_t(ELLEN_INF2, 0, true);
	}

	void initThread(const int tid) {
		last_result_threadlocal = (search_result_t *)new search_result_t();
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
	char *name() { return "BST Unbalanced Ellen"; }

	void print() { };
	unsigned long long size() { return size_rec(root) - 2; };

private:
	union info_t;
	typedef info_t* update_t; // FIXME quite a few CASes done on this data

	struct node_t {
		K key;
		V value;
		update_t update;
		node_t *left, *right;
		bool leaf;

		node_t(const K& key, const V& value, bool leaf) {
			this->key = key;
			this->value = value;
			this->leaf = leaf;
			this->update = NULL;
			this->left = this->right = NULL;
		};
	};

	struct iinfo_t {
		node_t *p,
		       *new_internal,
		       *l;

		iinfo_t(node_t *p, node_t *ni, node_t *l) {
			this->p = p;
			this->new_internal = ni;
			this->l = l;
		};
	};
	
	struct dinfo_t {
		node_t *gp,
		        *p,
		        *l;
		update_t pupdate;

		dinfo_t(node_t *gp, node_t *p, node_t *l, update_t u) {
			this->gp = gp;
			this->p = p;
			this->l = l;
			this->pupdate = u;
		};
	};
	
	union info_t {
		iinfo_t iinfo;
		dinfo_t dinfo;
		uint8_t padding[64];
	};
	
	struct search_result_t {
		node_t *gp,
		       *p,
		       *l;
		update_t pupdate,
		         gpupdate;

		search_result_t() {
			gp = p = l = NULL;
			pupdate = gpupdate = NULL;
		};
	};

	node_t *root;

private:
#undef GETFLAG
#undef FLAG
#undef UNFLAG
	inline uint64_t GETFLAG(update_t ptr) {
		return ((uint64_t)ptr) & 3;
	}
	
	inline uint64_t FLAG(update_t ptr, uint64_t flag) {
		return (((uint64_t)ptr) & 0xfffffffffffffffc) | flag;
	}
	
	inline uint64_t UNFLAG(update_t ptr) {
		return (((uint64_t)ptr) & 0xfffffffffffffffc);
	}

	search_result_t *search(const K& key)
	{
		search_result_t *last_result = (search_result_t *)last_result_threadlocal;
		search_result_t res;
	
		res.l = root;
		while (!(res.l->leaf)) {
			res.gp = res.p;
			res.p = res.l;
			res.gpupdate = res.pupdate;
			res.pupdate = res.p->update;
			if (key < res.l->key) res.l = (node_t*) res.p->left;
			else                  res.l = (node_t*) res.p->right;
		}
		last_result->gp = res.gp;
		last_result->p = res.p;
		last_result->l = res.l;
		last_result->gpupdate = res.gpupdate;
		last_result->pupdate = res.pupdate;
		return last_result;
	}

	const V lookup_helper(const K& key) {
		search_result_t *result = search(key);
		if (result->l->key == key) return result->l->value;
		else return this->NO_VALUE;
	}

	int cas_child(node_t *parent, node_t *old, node_t *new_node){
		if (new_node->key < parent->key) {
			if (CAS_PTR(&(parent->left), old, new_node) == old) return 1;
			return 0;
		} else {
			if (CAS_PTR(&(parent->right), old, new_node) == old) return 1;
			return 0;
		}
	}
	
	void help_insert(info_t *op) {
		int i = cas_child(op->iinfo.p, op->iinfo.l, op->iinfo.new_internal);
		void *dummy = CAS_PTR(&(op->iinfo.p->update), FLAG(op,STATE_IFLAG),
		                                              FLAG(op,STATE_CLEAN));
	}

	bool help_delete(info_t *op)
	{
		update_t result; 
		result = CAS_PTR(&(op->dinfo.p->update), op->dinfo.pupdate, FLAG(op,STATE_MARK));
		if ((result == op->dinfo.pupdate) || (result == ((info_t*)FLAG(op,STATE_MARK)))) {
			help_marked(op);
			return true;
		} else {
			help(result);
			void *dummy = CAS_PTR(&(op->dinfo.gp->update), FLAG(op,STATE_DFLAG),
			                                               FLAG(op,STATE_CLEAN));
			return false;
		}
	}
	
	void help_marked(info_t *op)
	{
		node_t *other;
		if (op->dinfo.p->right == op->dinfo.l) other = (node_t*)op->dinfo.p->left;
		else other = (node_t*)op->dinfo.p->right; 
		int i = cas_child(op->dinfo.gp, op->dinfo.p, other);
		void *dummy = CAS_PTR(&(op->dinfo.gp->update), FLAG(op,STATE_DFLAG),
		                                               FLAG(op,STATE_CLEAN));
	}
	
	void help(update_t u)
	{
		if (GETFLAG(u) == STATE_IFLAG)      help_insert((info_t*)UNFLAG(u));
		else if (GETFLAG(u) == STATE_MARK)  help_marked((info_t*)UNFLAG(u));
		else if (GETFLAG(u) == STATE_DFLAG) help_delete((info_t*)UNFLAG(u)); 
	}
	
	int do_insert(const K& key, const V& value, node_t **new_node,
	              node_t **new_sibling, node_t **new_internal, 
	              search_result_t *search_result)
	{
		search_result_t *last_result = (search_result_t *)last_result_threadlocal;
		info_t *op;
		update_t result;
	
		if (GETFLAG(search_result->pupdate) != STATE_CLEAN) {
			help(search_result->pupdate);
		} else {
			if (*new_node == NULL) {
				*new_node = new node_t(key, value, true); 
				*new_sibling = new node_t(search_result->l->key, search_result->l->value, true);
				*new_internal = new node_t(MAX(key,search_result->l->key), 0, false);
			}
			(*new_sibling)->key = search_result->l->key;
			(*new_sibling)->value = search_result->l->value;
			(*new_sibling)->leaf = true;
			(*new_internal)->key = MAX(key, search_result->l->key);
			(*new_internal)->value = this->NO_VALUE;
			(*new_internal)->leaf = false;
	
			if ((*new_node)->key < (*new_sibling)->key) {
				(*new_internal)->left = *new_node;
				(*new_internal)->right = *new_sibling;
			} else {
				(*new_internal)->left = *new_sibling;
				(*new_internal)->right = *new_node;
			}
			op = (info_t *)new iinfo_t(search_result->p, *new_internal, search_result->l);
			result = CAS_PTR(&(search_result->p->update), search_result->pupdate,
			                 FLAG(op,STATE_IFLAG));
			if (result == search_result->pupdate) {
				help_insert(op);
				return 1;
			} else {
				help(result);
			}
		}
		return 0;
	}
	
	const V insert_helper(const K& key, const V& value)
	{
		node_t *new_internal = NULL, *new_sibling = NULL, *new_node = NULL;
		search_result_t *search_result;
	
		while(1) {
			search_result = search(key);
			if (search_result->l->key == key)
				return search_result->l->value;
			if (do_insert(key, value, &new_node, &new_sibling, &new_internal, search_result))
				return this->NO_VALUE;
		}
	}

	int do_delete(search_result_t *search_result, V& del_val)
	{
		update_t result;
		info_t *op;
	
		del_val = search_result->l->value;
		if (GETFLAG(search_result->gpupdate) != STATE_CLEAN) {
			help(search_result->gpupdate);
		} else if (GETFLAG(search_result->pupdate) != STATE_CLEAN){
			help(search_result->pupdate);
		} else {
			op = (info_t *)new dinfo_t(search_result->gp, search_result->p, 
			                           search_result->l, search_result->pupdate);
			result = CAS_PTR(&(search_result->gp->update), search_result->gpupdate,
			                                               FLAG(op,STATE_DFLAG));
			if (result == search_result->gpupdate) {
				if (help_delete(op) == true) return 1;
			} else {
				help(result);
			}
		}
		return 0;
	}
	
	const V delete_helper(const K& key)
	{
		search_result_t *search_result;
		V del_val;
		while (1) {
			search_result = search(key); 
			if (search_result->l->key != key)      return this->NO_VALUE;
			if (do_delete(search_result, del_val)) return del_val;
		}
	}

	//static int bst_update(skey_t key, sval_t value, node_t *root)
	//{
	//	int op_is_insert = -1;
	//	node_t *new_internal = NULL, *new_sibling = NULL, *new_node = NULL;
	//	search_result_t* search_result;
	//
	//	while (1) {
	//		search_result = bst_search(key,root); 
	//		if (op_is_insert == -1) {
	//			if (search_result->l->key == key) op_is_insert = 0;
	//			else                              op_is_insert = 1;
	//		}
	//
	//		if (op_is_insert) {
	//			if (search_result->l->key == key)
	//				return 0;
	//			if (do_bst_insert(key, value, &new_node, &new_sibling, &new_internal, search_result))
	//				return 1;
	//		} else {
	//			if (search_result->l->key!=key)
	//				return 2;
	//			if (do_bst_delete(search_result))
	//				return 3;
	//		}
	//	}
	//	return 0;
	//}

	unsigned long long size_rec(node_t *node){
			if (node->leaf) return 1;
			return (size_rec(node->right) + size_rec(node->left));
	}
	
	int total_paths, total_nodes, bst_violations;
	int min_path_len, max_path_len;
	void validate_rec(node_t *root, int _th)
	{
		if (!root) return;
	
		node_t *left = root->left;
		node_t *right = root->right;
	
		if (root->key < ELLEN_INF1) {
			total_nodes++;
			_th++;
		}
	
		/* BST violation? */
		if (left && left->key >= root->key)
			bst_violations++;
		if (right && right->key < root->key)
			bst_violations++;
	
		/* We found a path (a node with at least one sentinel child). */
		if (!left && !right && root->key < ELLEN_INF1) {
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
		printf("  Min/max paths length: %d/%d\n", min_path_len, max_path_len);
		printf("\n");
	
		return check_bst;
	}
};

#define BST_UNB_ELLEN_TEMPL template<typename K, typename V>
#define BST_UNB_ELLEN_FUNCT bst_unb_ellen<K,V>

BST_UNB_ELLEN_TEMPL
bool BST_UNB_ELLEN_FUNCT::contains(const int tid, const K& key)
{
	const V ret = lookup_helper(key);
	return ret != this->NO_VALUE;
}

BST_UNB_ELLEN_TEMPL
const std::pair<V,bool> BST_UNB_ELLEN_FUNCT::find(const int tid, const K& key)
{
	const V ret = lookup_helper(key);
	return std::pair<V,bool>(ret, ret != this->NO_VALUE);
}

BST_UNB_ELLEN_TEMPL
int BST_UNB_ELLEN_FUNCT::rangeQuery(const int tid, const K& lo, const K& hi,
                      std::vector<std::pair<K,V>> kv_pairs)
{
	return 0;
}

BST_UNB_ELLEN_TEMPL
const V BST_UNB_ELLEN_FUNCT::insert(const int tid, const K& key, const V& val)
{
	return insertIfAbsent(tid, key, val);
}

BST_UNB_ELLEN_TEMPL
const V BST_UNB_ELLEN_FUNCT::insertIfAbsent(const int tid, const K& key, const V& val)
{
	return insert_helper(key, val);
}

BST_UNB_ELLEN_TEMPL
const std::pair<V,bool> BST_UNB_ELLEN_FUNCT::remove(const int tid, const K& key)
{
	const V ret = delete_helper(key);
	return std::pair<V,bool>(ret, ret != this->NO_VALUE);
}

BST_UNB_ELLEN_TEMPL
bool BST_UNB_ELLEN_FUNCT::validate()
{
	return validate_helper();
}
