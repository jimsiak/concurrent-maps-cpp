#pragma once

#include "map_if.h"

#include "contention-adaptive/ca-locks.h"
#include "seq/treap.h"
#include "seq/bst_unb_int.h"
#include "seq/bst_unb_pext.h"
#include "seq/bst_unb_ext.h"
#include "seq/bst_avl_int.h"
#include "seq/bst_avl_pext.h"
#include "seq/bst_avl_ext.h"
#include "seq/bst_rbt_int.h"
#include "seq/bst_rbt_ext.h"
#include "seq/btree.h"
#include "seq/abtree.h"

#include "locks/bst_avl_bronson.h"
#include "locks/bst_avl_drachsler.h"
#include "locks/bst_avl_cf.h"
#include "locks/bst_unb_ext_hohlocks.h"

#include "lock-free/bst_unb_natarajan.h"
#include "lock-free/bst_unb_ellen.h"
//#include "lock-free/bst_unb_howley.h"
//#include "lock-free/ist_brown/brown_ext_ist_lf_impl.h"
//#include "lock-free/abtree_brown/brown_ext_abtree_lf_impl.h"
//#include "lock-free/bwtree_wang/wang.h"

#include "cg-sync/cg_ds.h"

#include "rcu-htm/rcu-htm.h"

template <typename K, typename V>
static Map<K,V> *createMap(std::string& type, std::string& sync_type)
{

	Map<K,V> *map;

	//> Sequential data structures
	if (type == "treap-seq")
		map = new Treap<K,V>(-1, NULL, 88);
	else if (type == "bst-unb-int")
		map = new bst_unb_int<K,V>(-1, NULL, 88);
	else if (type == "bst-unb-pext")
		map = new bst_unb_pext<K,V>(-1, NULL, 88);
	else if (type == "bst-unb-ext")
		map = new bst_unb_ext<K,V>(-1, NULL, 88);
	else if (type == "bst-avl-int")
		map = new bst_avl_int<K,V>(-1, NULL, 88);
	else if (type == "bst-avl-pext")
		map = new bst_avl_pext<K,V>(-1, NULL, 88);
	else if (type == "bst-avl-ext")
		map = new bst_avl_ext<K,V>(-1, NULL, 88);
	else if (type == "bst-rbt-int")
		map = new bst_rbt_int<K,V>(-1, NULL, 88);
	else if (type == "bst-rbt-ext")
		map = new bst_rbt_ext<K,V>(-1, NULL, 88);
	else if (type == "btree")
		map = new btree<K,V>(-1, NULL, 88);
	else if (type == "abtree")
		map = new abtree<K,V>(-1, NULL, 88);
	//> Lock-based
	else if (type == "bst-avl-bronson")
		map = new bst_avl_bronson<K,V>(-1, NULL, 88);
	else if (type == "bst-avl-drachsler")
		map = new bst_avl_drachsler<K,V>(-1, NULL, 88);
	else if (type == "bst-avl-cf")
		map = new bst_avl_cf<K,V>(-1, NULL, 88);
	else if (type == "bst-unb-ext-hohlocks")
		map = new bst_unb_ext_hohlocks<K,V>(-1, NULL, 88);
	//> Lock-free
	else if (type == "bst-unb-natarajan")
		map = new bst_unb_natarajan<K,V>(-1, NULL, 88);
	else if (type == "bst-unb-ellen")
		map = new bst_unb_ellen<K,V>(-1, NULL, 88);
//	else if (type == "bst-unb-howley")
//		map = new bst_unb_howley<K,V>(-1, NULL, 88);
//	else if (type == "ist-brown")
//		map = new ist_brown<K,V>(-1, NULL, 88);
//	else if (type == "abtree-brown")
//		map = new abtree_brown<K,V>(-1, NULL, 88);
//	else if (type == "bwtree-wang")
//		map = new bwtree_wang<K,V>(-1, NULL, 88);
	else
		map = NULL;


	if (sync_type == "cg-sync")
		map = new cg_ds<K,V>(-1, NULL, 88, map);
	else if (sync_type == "rcu-htm")
		map = new rcu_htm<K,V>(-1, NULL, 88, map);

	return map;
}
