/**
 * Preliminary C++ implementation of unbalanced binary search tree using LLX/SCX.
 * 
 * Copyright (C) 2014 Trevor Brown
 * 
 * Why is this code so long?
 * - Because this file defines FOUR implementations
 *   (1) transactional lock elision (suffix _tle)
 *   (2) hybrid tm based implementation (suffix _tm)
 *   (3) 3-path implementation (suffixes _fallback, _middle, _fast)
 *   (4) global locking (suffix _lock_search_inplace)
 * - Because the LLX and SCX synchronization primitives are implemented here
 *   (including memory reclamation for SCX records)
 */

#pragma once

#include <string>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <atomic>
#include <set>
#include <setjmp.h>
#include <csignal>
#include <unistd.h>
#include <sys/types.h>
#include <pthread.h>
#include <stdexcept>
#include <bitset>
#include <immintrin.h>
#include <cassert>
#include <cstdlib>

#include "../../map_if.h"

using namespace std;

//#define IF_FAIL_TO_PROTECT_SCX(info, tid, _obj, arg2, arg3) \
//    info.obj = _obj; \
//    info.ptrToObj = (void * volatile *) arg2; \
//    info.nodeContainingPtrToObjIsMarked = arg3; \
//    if (_obj != dummy)
#define IF_FAIL_TO_PROTECT_NODE(info, tid, _obj, arg2, arg3) \
    info.obj = _obj; \
    info.ptrToObj = (void * volatile *) arg2; \
    info.nodeContainingPtrToObjIsMarked = arg3; \
    if (_obj != root)


#define UPDATE_FUNCTION(name) \
	bool (bst_brown<K,V>::*name)(ReclamationInfo<K,V> * const, \
	                             const int, void **, void **)
#define CAST_UPDATE_FUNCTION(name) (UPDATE_FUNCTION()) &bst_brown<K,V>::name

/**
* this is what LLX returns when it is performed on a leaf.
* the important qualities of this value are:
*      - it is not NULL
*      - it cannot be equal to any pointer to an scx record
*/
#define LLX_RETURN_IS_LEAF ((void*) 1)

#define ABORT_STATE_INIT(i, freezeCount) (SCXRecord<K,V>::STATE_ABORTED | ((i)<<2) | ((freezeCount)<<16))
#define STATE_GET_HIGHEST_INDEX_REACHED(state) (((state) & ((1<<16)-1))>>2)
#define STATE_GET_REFCOUNT(state) ((state)>>16)
#define STATE_REFCOUNT_UNIT (1<<16)

static const int MAX_NODES = 6;
static const int NUMBER_OF_PATHS = 3;
static const int PATH_FAST_HTM = 0;
static const int PATH_SLOW_HTM = 1;
static const int PATH_FALLBACK = 2;

static const int ABORT_PROCESS_ON_FALLBACK = 3;
static const int ABORT_NODE_POINTER_CHANGED = 12;
static const int ABORT_LLX_FAILED = 13;

class bst_retired_info {
public:
    void * obj;
    void * volatile * ptrToObj;
    volatile bool * nodeContainingPtrToObjIsMarked;
    bst_retired_info(
            void * _obj,
            void * volatile * _ptrToObj,
            volatile bool * _nodeContainingPtrToObjIsMarked)
            : obj(_obj),
              ptrToObj(_ptrToObj),
              nodeContainingPtrToObjIsMarked(_nodeContainingPtrToObjIsMarked) {}
    bst_retired_info() {}
};

template <class K, class V> class SCXRecord;

template <class K, class V>
class Node {
public:
    V value;
    K key;
    SCXRecord<K,V> * volatile scxRecord;
    volatile bool marked;
    Node<K,V> * volatile left;
	Node<K,V> * volatile right;
	char padding[64 - 40];
    
    Node() {
        // left blank for efficiency with custom allocator
    }
    Node(const Node& node) {
        // left blank for efficiency with custom allocator
    }

    K getKey() { return key; }
    V getValue() { return value; }
};

template <class K, class V>
class ReclamationInfo {
public:
    int type;
    Node<K,V> *nodes[MAX_NODES];
    void *llxResults[MAX_NODES];
    int state;
    int numberOfNodes;
    int numberOfNodesToFreeze;
    int numberOfNodesToReclaim;
    int numberOfNodesAllocated;
    int path;
    bool capacityAborted[NUMBER_OF_PATHS];
    int lastAbort;
};

string const NAME_OF_TYPE[33] = {
    string("INS"),
    string("DEL"),
    string("BLK"),
    string("RB1"),
    string("RB2"),
    string("PUSH"),
    string("W1"),
    string("W2"),
    string("W3"),
    string("W4"),
    string("W5"),
    string("W6"),
    string("W7"),
    string("DBL1"),
    string("DBL2"),
    string("DBL3"),
    string("DBL4"),
    string("RB1SYM"),
    string("RB2SYM"),
    string("PUSHSYM"),
    string("W1SYM"),
    string("W2SYM"),
    string("W3SYM"),
    string("W4SYM"),
    string("W5SYM"),
    string("W6SYM"),
    string("W7SYM"),
    string("DBL1SYM"),
    string("DBL2SYM"),
    string("DBL3SYM"),
    string("DBL4SYM"),
    string("REPLACE"),
    string("NOOP")
};

template <class K, class V>
class SCXRecord {
public:
    const static int TYPE_FIND      = -1;
    const static int TYPE_INS       = 0;
    const static int TYPE_DEL       = 1;
    const static int TYPE_BLK       = 2;
    const static int TYPE_RB1       = 3;
    const static int TYPE_RB2       = 4;
    const static int TYPE_PUSH      = 5;
    const static int TYPE_W1        = 6;
    const static int TYPE_W2        = 7;
    const static int TYPE_W3        = 8;
    const static int TYPE_W4        = 9;
    const static int TYPE_W5        = 10;
    const static int TYPE_W6        = 11;
    const static int TYPE_W7        = 12;
    const static int TYPE_DBL1      = 13;
    const static int TYPE_DBL2      = 14;
    const static int TYPE_DBL3      = 15;
    const static int TYPE_DBL4      = 16;
    const static int TYPE_RB1SYM    = 17;
    const static int TYPE_RB2SYM    = 18;
    const static int TYPE_PUSHSYM   = 19;
    const static int TYPE_W1SYM     = 20;
    const static int TYPE_W2SYM     = 21;
    const static int TYPE_W3SYM     = 22;
    const static int TYPE_W4SYM     = 23;
    const static int TYPE_W5SYM     = 24;
    const static int TYPE_W6SYM     = 25;
    const static int TYPE_W7SYM     = 26;
    const static int TYPE_DBL1SYM   = 27;
    const static int TYPE_DBL2SYM   = 28;
    const static int TYPE_DBL3SYM   = 29;
    const static int TYPE_DBL4SYM   = 30;
    const static int TYPE_REPLACE   = 31;
    const static int TYPE_NOOP      = 32;
    const static int NUM_OF_OP_TYPES = 33;

    const static int STATE_INPROGRESS = 0;
    const static int STATE_COMMITTED = 1;
    const static int STATE_ABORTED = 2;
    
    volatile bool allFrozen;
    char numberOfNodes, numberOfNodesToFreeze;
    volatile int state; // state of the scx
    Node<K,V> *nodes[MAX_NODES];                // array of pointers to nodes ; these are CASd to NULL as pointers nodes[i]->scxPtr are changed so that they no longer point to this scx record.
    SCXRecord<K,V> *scxRecordsSeen[MAX_NODES];  // array of pointers to scx records
    Node<K,V> *newNode;
    Node<K,V> * volatile * volatile field;

    SCXRecord() {              // create an inactive operation (a no-op) [[ we do this to avoid the overhead of inheritance ]]
        // left blank for efficiency with custom allocators
    }

    SCXRecord(const SCXRecord<K,V>& op) {
        // left blank for efficiency with custom allocators for efficiency with custom allocators
    }

    K getSubtreeKey() { return nodes[1].key; }
};

template <class K, class V>
class bst_brown : public Map<K,V> {

	const int MAX_FAST_HTM_RETRIES;
	const int MAX_SLOW_HTM_RETRIES;

private:
    Node<K,V> *root;
	char pad1[64];
    SCXRecord<K,V> *dummy;  // actually const
	char pad2[64];
	pthread_spinlock_t lock;
	char pad3[64];
    
    atomic_uint numFallback; // number of processes on the fallback path

	#define MAX_TID_POW2 128
	#define PREFETCH_SIZE_WORDS 24
    #define VERSION_NUMBER(tid) (version[(tid)*PREFETCH_SIZE_WORDS])
    #define INIT_VERSION_NUMBER(tid) (VERSION_NUMBER(tid) = ((tid << 1) | 1))
    #define NEXT_VERSION_NUMBER(tid) (VERSION_NUMBER(tid) += (MAX_TID_POW2 << 1))
    #define IS_VERSION_NUMBER(infoPtr) (((long) (infoPtr)) & 1)
    long version[MAX_TID_POW2*PREFETCH_SIZE_WORDS];

public:
    bst_brown(const K& _NO_KEY, const V& _NO_VALUE, const int numProcesses,
	          const int fast_htm_retries = 10, const int slow_htm_retries = 10)
	  : Map<K,V>(_NO_KEY, _NO_VALUE),
	    MAX_FAST_HTM_RETRIES(fast_htm_retries),
	    MAX_SLOW_HTM_RETRIES(slow_htm_retries)
	{
		const int tid = 0;
		numFallback = 0;
        dummy = allocateSCXRecord(tid);
        dummy->state = SCXRecord<K,V>::STATE_ABORTED; // this is a NO-OP, so it shouldn't start as InProgress; aborted is just more efficient than committed, since we won't try to help marked leaves, which always have the dummy scx record...
		Node<K,V> *rootLeft = allocateNode(tid);
		initializeNode(tid, rootLeft, _NO_KEY, _NO_VALUE, NULL, NULL);
		root = allocateNode(tid);
		initializeNode(tid, root, _NO_KEY, _NO_VALUE, rootLeft, NULL);
		pthread_spin_init(&lock, PTHREAD_PROCESS_SHARED);
//        numSlowHTM = 0;

		for (int tid=0;tid<numProcesses;++tid) {
			INIT_VERSION_NUMBER(tid);
//			GET_ALLOCATED_SCXRECORD_PTR(tid) = NULL;
		}
    }

    void initThread(const int tid) {}
    void deinitThread(const int tid) {}
    
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
	char *name() {
		if (MAX_SLOW_HTM_RETRIES == -1 && MAX_FAST_HTM_RETRIES == -1)
			return "BST Unbalanced Brown (LLX/SCX)";
		else
			return "BST Unbalanced Brown (3-Path)";
	}

	void print() {};
//	unsigned long long size() { return size_rec(root) - 2; };

private:

	void htmWrapper(UPDATE_FUNCTION(update_for_fastHTM),
	                UPDATE_FUNCTION(update_for_slowHTM),
	                UPDATE_FUNCTION(update_for_fallback),
	                const int tid, void **input, void **output)
	{
	    ReclamationInfo<K,V> info;
	    info.path = (MAX_FAST_HTM_RETRIES >= 0 ? PATH_FAST_HTM :
		             MAX_SLOW_HTM_RETRIES >= 0 ? PATH_SLOW_HTM : PATH_FALLBACK);
	    int attempts = 0;
	    bool finished = 0;
	    info.capacityAborted[PATH_FAST_HTM] = 0;
	    info.capacityAborted[PATH_SLOW_HTM] = 0;
	    info.lastAbort = 0;
	    for (;;) {
	        switch (info.path) {
	            case PATH_FAST_HTM:
	                finished = (this->*update_for_fastHTM)(&info, tid, input, output);
	                if (finished) {
	                    return;
	                } else {
	                    // check if we should change paths
	                    ++attempts;
	                    // TODO: move to middle immediately if a process is on the fallback path
	                    if (attempts > MAX_FAST_HTM_RETRIES) {
	                        attempts = 0;
	                        // check if we aren't allowing slow htm path
	                        if (MAX_SLOW_HTM_RETRIES < 0) {
	                            info.path = PATH_FALLBACK;
	                            numFallback.fetch_add(1);
	                        } else {
	                            info.path = PATH_SLOW_HTM;
	                        }
	                    /* MOVE TO THE MIDDLE PATH IMMEDIATELY IF SOMEONE IS ON THE FALLBACK PATH */ \
	                    } else if ((info.lastAbort >> 24) == ABORT_PROCESS_ON_FALLBACK && MAX_SLOW_HTM_RETRIES >= 0) { /* DEBUG */
	                        attempts = 0;
	                        info.path = PATH_SLOW_HTM;
	                        //continue;
	                    /* if there is no middle path, wait for the fallback path to be empty */ \
	                    } else if (MAX_SLOW_HTM_RETRIES < 0) {
	                        while (numFallback.load(memory_order_relaxed) > 0) { __asm__ __volatile__("pause;"); }
	                    }
	                }
	                break;
	            case PATH_SLOW_HTM:
	                finished = (this->*update_for_slowHTM)(&info, tid, input, output);
	                if (finished) {
	                    return;
	                } else {
	                    // check if we should change paths
	                    ++attempts;
	                    if (attempts > MAX_SLOW_HTM_RETRIES) {
	                        attempts = 0;
	                        info.path = PATH_FALLBACK;
	                        if (MAX_FAST_HTM_RETRIES >= 0 || MAX_SLOW_HTM_RETRIES >= 0) numFallback.fetch_add(1);
	                    }
	                }
	                break;
	            case PATH_FALLBACK:
	                finished = (this->*update_for_fallback)(&info, tid, input, output);
	                if (finished) {
	                    if (MAX_FAST_HTM_RETRIES >= 0 || MAX_SLOW_HTM_RETRIES >= 0) numFallback.fetch_add(-1);
	                    return;
	                } else {
	                }
	                break;
	            default:
	                cout<<"reached impossible switch case"<<endl;
	                exit(-1);
	                break;
	        }
	    }
	}

	const V insert_helper(const int tid, const K& key, const V& val)
	{
	    bool onlyIfAbsent = false;
	    V result = this->NO_VALUE;
	    void *input[] = {(void*) &key, (void*) &val, (void*) &onlyIfAbsent};
	    void *output[] = {(void*) &result};

	    htmWrapper(CAST_UPDATE_FUNCTION(insert_txn_search_inplace),
	               CAST_UPDATE_FUNCTION(insert_txn_search_replace_markingw_infowr),
	               CAST_UPDATE_FUNCTION(insert_search_llx_scx), tid, input, output);
	    return result;
	}

	const pair<V,bool> delete_helper(const int tid, const K& key)
	{
	    V result = this->NO_VALUE;
	    void *input[] = {(void*) &key};
	    void *output[] = {(void*) &result};
	    htmWrapper(CAST_UPDATE_FUNCTION(erase_txn_search_inplace),
	               CAST_UPDATE_FUNCTION(erase_txn_search_replace_markingw_infowr),
	               CAST_UPDATE_FUNCTION(erase_search_llx_scx), tid, input, output);
	    return pair<V,bool>(result, (result != this->NO_VALUE));
	}



private:
	Node<K,V> *allocateNode(const int tid)
	{
	    Node<K,V> *newnode = new Node<K,V>();
	    if (newnode == NULL) {
			std::cerr << "ERROR: could not allocate node" << std::endl;
	        exit(-1);
	    }
	    return newnode;
	}

	void initializeNode(const int tid, Node<K,V> * const newnode,
	                    const K& key, const V& value,
	                    Node<K,V> * const left, Node<K,V> * const right)
	{
	    newnode->key = key;
	    newnode->value = value;
	    // note: synchronization is not necessary for the following accesses,
	    // since a memory barrier will occur before this object becomes reachable
	    // from an entry point to the data structure.
	    newnode->left = left;
	    newnode->right = right;
	    newnode->scxRecord = dummy;
	    newnode->marked = false;
	}

private:
	const V lookup_helper(const int tid, const K& key)
	{
	    bst_retired_info info;
	    Node<K,V> *p;
	    Node<K,V> *l;

	    for (;;) {
	            // root is never retired, so we don't need to call
	            // protectPointer before accessing its child pointers
	            p = root->left;
	            IF_FAIL_TO_PROTECT_NODE(info, tid, p, &root->left, &root->marked)
	                continue; /* retry */ 

	            assert(p != root);

	            l = p->left;
	            if (l == NULL)
	                return this->NO_VALUE; // success

	            IF_FAIL_TO_PROTECT_NODE(info, tid, l, &p->left, &p->marked)
	                continue; /* retry */ 
	
	            while (l->left != NULL) {
	                p = l; // note: the new p is currently protected
	                assert(p->key != this->INF_KEY);
	                if (key < p->key) {
	                    l = p->left;
	                    IF_FAIL_TO_PROTECT_NODE(info, tid, l, &p->left, &p->marked)
	                        continue; /* retry */ 
	                } else {
	                    l = p->right;
	                    IF_FAIL_TO_PROTECT_NODE(info, tid, l, &p->right, &p->marked)
	                        continue; /* retry */ 
	                }
	            }
	            if (key == l->key) return l->value;
	            else               this->NO_VALUE;
	    }
	    return this->NO_VALUE;
	}


private:
	bool insert_txn_search_inplace(ReclamationInfo<K,V> *const info,
	                               const int tid, void **input, void **output)
	{
	    const K& key = *((const K*) input[0]);
	    const V& val = *((const V*) input[1]);
	    const bool onlyIfAbsent = *((const bool*) input[2]);
	    V *result = (V*) output[0];
	
		Node<K,V> *newNode0 = allocateNode(tid);
		Node<K,V> *newNode1 = allocateNode(tid);
	    initializeNode(tid, newNode0, key, val, NULL, NULL);
	TXN1: int attempts = MAX_FAST_HTM_RETRIES;
	    int status = _xbegin();
	    if (status == _XBEGIN_STARTED) {
	        if (info->path == PATH_FAST_HTM && numFallback.load(memory_order_relaxed) > 0) _xabort(ABORT_PROCESS_ON_FALLBACK);
	        Node<K,V> *p = root, *l;
	        l = root->left;
	        if (l->left != NULL) { // the tree contains some node besides sentinels...
	            p = l;
	            l = l->left;    // note: l must have key infinity, and l->left must not.
	            while (l->left != NULL) {
	                p = l;
	                if (key < p->key) l = p->left;
	                else              l = p->right;
	            }
	        }

	        // if we find the key in the tree already
	        if (key == l->key) {
	            if (onlyIfAbsent) {
	                _xend();
	                *result = val; // for insertIfAbsent, we don't care about the particular value, just whether we inserted or not. so, we use val to signify not having inserted (and NO_VALUE to signify having inserted).
	                return true; // success
	            } else {
	                *result = l->value;
	                l->value = val;
	                _xend();
	                return true;
	            }
	        } else {
	            if (l->key == this->INF_KEY || key < l->key)
	                initializeNode(tid, newNode1, l->key, l->value, newNode0, l);
	            else
	                initializeNode(tid, newNode1, key, val, l, newNode0);
	            *result = this->NO_VALUE;
	
	            Node<K,V> *pleft = p->left;
	            if (l == pleft) p->left = newNode1;
	            else            p->right = newNode1;
	            _xend();
	            
	            // do memory reclamation and allocation
//	            REPLACE_ALLOCATED_NODE(tid, 0);
//	            REPLACE_ALLOCATED_NODE(tid, 1);
	            
	            return true;
	        }
	    } else {
	aborthere:
	        info->lastAbort = status;
//	        IF_ALWAYS_RETRY_WHEN_BIT_SET if (status & _XABORT_RETRY) { this->counters->pathFail[info->path]->inc(tid); this->counters->htmRetryAbortRetried[info->path]->inc(tid); goto TXN1; }
	        return false;
	    }
	}

	bool insert_txn_search_replace_markingw_infowr(ReclamationInfo<K,V> * const info,
	                                      const int tid, void **input, void **output)
	{
	    const K& key = *((const K*) input[0]);
	    const V& val = *((const V*) input[1]);
	    const bool onlyIfAbsent = *((const bool*) input[2]);
	    V *result = (V*) output[0];
	    
		Node<K,V> *newNode0 = allocateNode(tid);
		Node<K,V> *newNode1 = allocateNode(tid);
	    SCXRecord<K,V>* scx = (SCXRecord<K,V>*) NEXT_VERSION_NUMBER(tid);
	TXN1: int attempts = MAX_FAST_HTM_RETRIES;
	    int status = _xbegin();
	    if (status == _XBEGIN_STARTED) {
	        if (info->path == PATH_FAST_HTM && numFallback.load(memory_order_relaxed) > 0) _xabort(ABORT_PROCESS_ON_FALLBACK);
	        Node<K,V> *p = root, *l;
	        l = root->left;
	        if (l->left != NULL) { // the tree contains some node besides sentinels...
	            p = l;
	            l = l->left;    // note: l must have key infinity, and l->left must not.
	            while (l->left != NULL) {
	                p = l;
	                if (key < p->key) l = p->left;
	                else              l = p->right;
	            }
	        }
	        // if we find the key in the tree already
	        if (key == l->key) {
	            if (onlyIfAbsent) {
	                _xend();
	                *result = val; // for insertIfAbsent, we don't care about the particular value, just whether we inserted or not. so, we use val to signify not having inserted (and NO_VALUE to signify having inserted).
	                return true; // success
	            }
	            Node<K,V> *pleft, *pright;
	            if ((info->llxResults[0] = llx_intxn_markingwr_infowr(tid, p, &pleft, &pright)) == NULL)
	                _xabort(ABORT_NODE_POINTER_CHANGED);

	            // assert l is a child of p
	            *result = l->value;
	            initializeNode(tid, newNode0, key, val, NULL, NULL);
	            p->scxRecord = scx;
	            l->marked = true;
	            (l == pleft ? p->left : p->right) = newNode0;
	            _xend();
	            
	            // do memory reclamation and allocation
//	            shmem->retire(tid, l);
//	            tryRetireSCXRecord(tid, (SCXRecord<K,V> *) info->llxResults[0], p);
//	            REPLACE_ALLOCATED_NODE(tid, 0);
	            
	            return true;
	        } else {
	            Node<K,V> *pleft, *pright;
	            if ((info->llxResults[0] = llx_intxn_markingwr_infowr(tid, p, &pleft, &pright)) == NULL)
	                _xabort(ABORT_NODE_POINTER_CHANGED);
	            // assert l is a child of p
	            initializeNode(tid, newNode0, key, val, NULL, NULL);
	            if (l->key == this->INF_KEY || key < l->key)
	                initializeNode(tid, newNode1, l->key, l->value, newNode0, l);
	            else
	                initializeNode(tid, newNode1, key, val, l, newNode0);
	            *result = this->NO_VALUE; 
	            p->scxRecord = scx;
	            (l == pleft ? p->left : p->right) = newNode1;
	            _xend();
	            
	            // do memory reclamation and allocation
//	            tryRetireSCXRecord(tid, (SCXRecord<K,V> *) info->llxResults[0], p);
//	            REPLACE_ALLOCATED_NODE(tid, 0);
//	            REPLACE_ALLOCATED_NODE(tid, 1);
	            
	            return true;
	        }
	    } else {
	aborthere:
	        info->lastAbort = status;
//	        IF_ALWAYS_RETRY_WHEN_BIT_SET if (status & _XABORT_RETRY) { this->counters->pathFail[info->path]->inc(tid); this->counters->htmRetryAbortRetried[info->path]->inc(tid); goto TXN1; }
	        return false;
	    }
	}

	bool insert_search_llx_scx(ReclamationInfo<K,V> * const info, const int tid,
	                           void **input, void **output)
	{
	    const K& key = *((const K*) input[0]);
	    const V& val = *((const V*) input[1]);
	    const bool onlyIfAbsent = *((const bool*) input[2]);
	    V *result = (V*) output[0];
	    
		Node<K,V> *newNode0 = allocateNode(tid);
		Node<K,V> *newNode1 = allocateNode(tid);

	    Node<K,V> *p = root;
	    Node<K,V> *l = root->left;
	    if (l->left != NULL) {
	        p = l;
	        l = l->left;
	        while (l->left != NULL) {
	            p = l;
	            if (key < p->key) l = p->left;
	            else              l = p->right;
	        }
	    }

	    // if we find the key in the tree already
	    if (key == l->key) {
	        if (onlyIfAbsent) {
	            *result = l->value;
	            return true;
	        }
	        Node<K,V> *pleft, *pright;
	        if ((info->llxResults[0] = llx(tid, p, &pleft, &pright)) == NULL)
	            return false;
	        if (l != pleft && l != pright)
	            return false;

	        *result = l->value;
	        initializeNode(tid, newNode0, key, val, NULL, NULL);
	        info->numberOfNodes = 2;
	        info->numberOfNodesToFreeze = 1;
	        info->numberOfNodesToReclaim = 1;
	        info->numberOfNodesAllocated = 1;
	        info->type = SCXRecord<K,V>::TYPE_REPLACE;
	        info->nodes[0] = p;
	        info->nodes[1] = l;
	        return scx(tid, info, (l == pleft ? &p->left : &p->right), newNode0);
	    } else {
	        Node<K,V> *pleft, *pright;
	        if ((info->llxResults[0] = llx(tid, p, &pleft, &pright)) == NULL)
	            return false;
	        if (l != pleft && l != pright)
	            return false;

	        // Compute the weight for the new parent node.
	        // If l is a sentinel then we must set its weight to one.
	        initializeNode(tid, newNode0, key, val, NULL, NULL);

	        if (l->key == this->INF_KEY || key < l->key)
	            initializeNode(tid, newNode1, l->key, l->value, newNode0, l);
	        else
	            initializeNode(tid, newNode1, key, val, l, newNode0);

	        *result = this->NO_VALUE;
	        info->numberOfNodes = 2;
	        info->numberOfNodesToReclaim = 0;
	        info->numberOfNodesToFreeze = 1; // only freeze nodes[0]
	        info->numberOfNodesAllocated = 2;//3;
	        info->type = SCXRecord<K,V>::TYPE_INS;
	        info->nodes[0] = p;
	        info->nodes[1] = l; // note: used as OLD value for CAS that changes p's child pointer (but is not frozen or marked)
	        return scx(tid, info, (l == pleft ? &p->left : &p->right), newNode1);
	    }
	}


	bool insert_lock_search_inplace(ReclamationInfo<K,V> * const info,
	                                const int tid, void **input, void **output)
	{
	    const K& key = *((const K*) input[0]);
	    const V& val = *((const V*) input[1]);
	    const bool onlyIfAbsent = *((const bool*) input[2]);
	    V *result = (V*) output[0];
	
	    pthread_spin_lock(&lock);
	    Node<K,V> *p = root, *l;
	    l = root->left;
	    if (l->left != NULL) { // the tree contains some node besides sentinels...
	        p = l;
	        l = l->left;    // note: l must have key infinity, and l->left must not.
	        while (l->left != NULL) {
	            p = l;
	            if (key < p->key) l = p->left;
	            else              l = p->right;
	        }
	    }
	    // if we find the key in the tree already
	    if (key == l->key) {
	        if (onlyIfAbsent) {
	            pthread_spin_unlock(&lock);
	            *result = val; // for insertIfAbsent, we don't care about the particular value, just whether we inserted or not. so, we use val to signify not having inserted (and NO_VALUE to signify having inserted).
	            return true; // success
	        } else {
	            *result = l->value;
	            l->value = val;
	            pthread_spin_unlock(&lock);
	            return true;
	        }
	    } else {
			Node<K,V> *newNode0 = allocateNode(tid);
			Node<K,V> *newNode1 = allocateNode(tid);
	        initializeNode(tid, newNode0, key, val, NULL, NULL);
	        if (l->key == this->INF_KEY || key < l->key)
	            initializeNode(tid, newNode1, l->key, l->value, newNode0, l);
	        else
	            initializeNode(tid, newNode1, key, val, l, newNode0);
	
	        Node<K,V> *pleft = p->left;            
	        if (l == pleft) p->left = newNode1;
			else            p->right = newNode1;
            pthread_spin_unlock(&lock);
	        *result = this->NO_VALUE;
	        
	        // do memory reclamation and allocation
//	        shmem->retire(tid, l);
//	        REPLACE_ALLOCATED_NODE(tid, 0);
//	        REPLACE_ALLOCATED_NODE(tid, 1);
	
	        return true;
	    }
	}

	bool erase_txn_search_inplace(ReclamationInfo<K,V> * const info,
	                              const int tid, void **input, void **output)
	{
		// input consists of: const K& key
	    const K& key = *((const K*) input[0]);
	    V *result = (V*) output[0];
	
	TXN1: int attempts = MAX_FAST_HTM_RETRIES;
	    int status = _xbegin();
	    if (status == _XBEGIN_STARTED) {
	        if (info->path == PATH_FAST_HTM && numFallback.load(memory_order_relaxed) > 0) _xabort(ABORT_PROCESS_ON_FALLBACK);
	        Node<K,V> *gp, *p, *l;
	        l = root->left;
	        if (l->left == NULL) {
	            _xend();
	            *result = this->NO_VALUE;
	            return true;
	        } // only sentinels in tree...
	        gp = root;
	        p = l;
	        l = p->left;    // note: l must have key infinity, and l->left must not.
	        while (l->left != NULL) {
	            gp = p;
	            p = l;
	            if (key < p->key) l = p->left;
	            else              l = p->right;
	        }
	        // if we fail to find the key in the tree
	        if (key != l->key) {
	            _xend();
	            *result = this->NO_VALUE;
	            return true; // success
	        } else {
	            Node<K,V> *gpleft, *gpright;
	            Node<K,V> *pleft, *pright;
	            Node<K,V> *sleft, *sright;
	            gpleft = gp->left;
	            gpright = gp->right;
	            pleft = p->left;
	            pright = p->right;
	            // assert p is a child of gp, l is a child of p
	            Node<K,V> *s = (l == pleft ? pright : pleft);
	            sleft = s->left;
	            sright = s->right;
	            if (p == gpleft) gp->left = s;
	            else             gp->right = s;
	            *result = l->value;
	            _xend();
	
	            // do memory reclamation and allocation
//	            shmem->retire(tid, p);
//	            shmem->retire(tid, l);
//	            tryRetireSCXRecord(tid, p->scxRecord, p);
	
	            return true;
	        }
	    } else { // transaction failed
	aborthere:
	        info->lastAbort = status;
//	        IF_ALWAYS_RETRY_WHEN_BIT_SET if (status & _XABORT_RETRY) { this->counters->pathFail[info->path]->inc(tid); this->counters->htmRetryAbortRetried[info->path]->inc(tid); goto TXN1; }
	        return false;
	    }
	}

	bool erase_txn_search_replace_markingw_infowr(ReclamationInfo<K,V> *const info,
	             const int tid, void **input, void **output)
	{
		// input consists of: const K& key
	    const K& key = *((const K*) input[0]);
	    V *result = (V*) output[0];
	
	    SCXRecord<K,V>* scx = (SCXRecord<K,V>*) NEXT_VERSION_NUMBER(tid);
		Node<K,V> *newNode = allocateNode(tid);
	    Node<K,V> *gp, *p, *l;
	TXN1: int attempts = MAX_FAST_HTM_RETRIES;
	    int status = _xbegin();
	    if (status == _XBEGIN_STARTED) {
	        if (info->path == PATH_FAST_HTM && numFallback.load(memory_order_relaxed) > 0) _xabort(ABORT_PROCESS_ON_FALLBACK);
	        l = root->left;
	        if (l->left == NULL) {
	            _xend();
	            *result = this->NO_VALUE;
	            return true;
	        } // only sentinels in tree...
	        gp = root;
	        p = l;
	        l = p->left;    // note: l must have key infinity, and l->left must not.
	        while (l->left != NULL) {
	            gp = p;
	            p = l;
	            if (key < p->key) l = p->left;
	            else              l = p->right;
	        }
	        // if we fail to find the key in the tree
	        if (key != l->key) {
	            _xend();
	            *result = this->NO_VALUE;
	            return true; // success
	        } else {
	            Node<K,V> *gpleft, *gpright;
	            Node<K,V> *pleft, *pright;
	            Node<K,V> *sleft, *sright;
	            if ((info->llxResults[0] = llx_intxn_markingwr_infowr(tid, gp, &gpleft, &gpright)) == NULL) _xabort(ABORT_LLX_FAILED);
	            if ((info->llxResults[1] = llx_intxn_markingwr_infowr(tid, p, &pleft, &pright)) == NULL) _xabort(ABORT_LLX_FAILED);
	            *result = l->value;
	            // Read fields for the sibling s of l
	            Node<K,V> *s = (l == pleft ? pright : pleft);
	            if ((info->llxResults[2] = llx_intxn_markingwr_infowr(tid, s, &sleft, &sright)) == NULL) _xabort(ABORT_LLX_FAILED);
	            // Now, if the op. succeeds, all structure is guaranteed to be just as we verified
	            initializeNode(tid, newNode, s->key, s->value, sleft, sright);

	            // scx
	            gp->scxRecord = scx;
	            p->scxRecord = scx;
	            p->marked = true;
	            s->scxRecord = scx;
	            s->marked = true;
	            //l->marked = true; // l is known to be a leaf, so it doesn't need to be marked
	            (p == gpleft ? gp->left : gp->right) = newNode;
	            _xend();
	            
	            // do memory reclamation and allocation
//	            shmem->retire(tid, p);
//	            shmem->retire(tid, s);
//	            shmem->retire(tid, l);
//	            tryRetireSCXRecord(tid, (SCXRecord<K,V> *) info->llxResults[0], gp);
//	            tryRetireSCXRecord(tid, (SCXRecord<K,V> *) info->llxResults[1], p);
//	            tryRetireSCXRecord(tid, (SCXRecord<K,V> *) info->llxResults[2], s);
//	            REPLACE_ALLOCATED_NODE(tid, 0);
	
	            return true;
	        }
	    } else {
	aborthere:
	        info->lastAbort = status;
//	        IF_ALWAYS_RETRY_WHEN_BIT_SET if (status & _XABORT_RETRY) { this->counters->pathFail[info->path]->inc(tid); this->counters->htmRetryAbortRetried[info->path]->inc(tid); goto TXN1; }
	        return false;
	    }
	}

	bool erase_search_llx_scx(ReclamationInfo<K,V> * const info, const int tid,
	                          void **input, void **output)
	{
		// input consists of: const K& key
	    const K& key = *((const K*) input[0]);
	    V *result = (V*) output[0];
	
	    Node<K,V> *gp, *p, *l;
	    l = root->left;
	    if (l->left == NULL) {
	        *result = this->NO_VALUE;
	        return true;
	    } // only sentinels in tree...
	    gp = root;
	    p = l;
	    l = p->left;    // note: l must have key infinity, and l->left must not.
	    while (l->left != NULL) {
	        gp = p;
	        p = l;
	        if (key < p->key) l = p->left;
	        else              l = p->right;
	    }

	    // if we fail to find the key in the tree
	    if (key != l->key) {
	        *result = this->NO_VALUE;
	        return true; // success
	    } else {
	        Node<K,V> *gpleft, *gpright;
	        Node<K,V> *pleft, *pright;
	        Node<K,V> *sleft, *sright;
	        if ((info->llxResults[0] = llx(tid, gp, &gpleft, &gpright)) == NULL) return false;
	        if (p != gpleft && p != gpright) return false;
	        if ((info->llxResults[1] = llx(tid, p, &pleft, &pright)) == NULL) return false;
	        if (l != pleft && l != pright) return false;
	        *result = l->value;
	        // Read fields for the sibling s of l
	        Node<K,V> *s = (l == pleft ? pright : pleft);
	        if ((info->llxResults[2] = llx(tid, s, &sleft, &sright)) == NULL) return false;
	        // Now, if the op. succeeds, all structure is guaranteed to be just as we verified
			Node<K,V> *newNode = allocateNode(tid);
	        initializeNode(tid, newNode, s->key, s->value, sleft, sright);
	        info->numberOfNodes = 4;
	        info->numberOfNodesToReclaim = 3;
	        info->numberOfNodesToFreeze = 3;
	        info->numberOfNodesAllocated = 1;
	        info->type = SCXRecord<K,V>::TYPE_DEL;
	        info->nodes[0] = gp;
	        info->nodes[1] = p;
	        info->nodes[2] = s;
	        info->nodes[3] = l;
	        bool retval = scx(tid, info, (p == gpleft ? &gp->left : &gp->right), newNode);
	        return retval;
	    }
	}


	bool erase_lock_search_inplace(ReclamationInfo<K,V> * const info,
	                               const int tid, void **input, void **output)
	{
		// input consists of: const K& key
	    const K& key = *((const K*) input[0]);
	    V *result = (V*) output[0];
	
	    pthread_spin_lock(&lock);
	    Node<K,V> *gp, *p, *l;
	    l = root->left;
	    if (l->left == NULL) {
			pthread_spin_unlock(&lock);
	        *result = this->NO_VALUE;
	        return true;
	    } // only sentinels in tree...
	    gp = root;
	    p = l;
	    l = p->left;    // note: l must have key infinity, and l->left must not.
	    while (l->left != NULL) {
	        gp = p;
	        p = l;
	        if (key < p->key) l = p->left;
	        else              l = p->right;
	    }
	    // if we fail to find the key in the tree
	    if (key != l->key) {
			pthread_spin_unlock(&lock);
	        *result = this->NO_VALUE;
	        return true; // success
	    } else {
	        Node<K,V> *gpleft, *gpright;
	        Node<K,V> *pleft, *pright;
	        gpleft = gp->left;
	        gpright = gp->right;
	        pleft = p->left;
	        pright = p->right;
	        // assert p is a child of gp, l is a child of p
	        Node<K,V> *s = (l == pleft ? pright : pleft);
	        if (p == gpleft) gp->left = s;
	        else             gp->right = s;
	        *result = l->value;
			pthread_spin_unlock(&lock);
	        
	        // do memory reclamation and allocation
//	        shmem->retire(tid, p);
//	        shmem->retire(tid, l);
//	        tryRetireSCXRecord(tid, p->scxRecord, p);
	
	        return true;
	    }
	}

private:
	/**
	 * LLX/SCX related functions.
	 **/

	SCXRecord<K,V> *allocateSCXRecord(const int tid)
	{
	    SCXRecord<K,V> *newop = new SCXRecord<K,V>();
	    if (newop == NULL) {
			std::cerr << "ERROR: could not allocate scx record" << std::endl;
	        exit(-1);
	    }
	    return newop;
	}

	SCXRecord<K,V> *initializeSCXRecord(const int tid, SCXRecord<K,V> * const newop,
	                                    ReclamationInfo<K,V> * const info,
	                                    Node<K,V> * volatile * const field,
	                                    Node<K,V> * const newNode)
	{
	    newop->newNode = newNode;
	    for (int i=0;i<info->numberOfNodes;++i)
	        newop->nodes[i] = info->nodes[i];
	    for (int i=0;i<info->numberOfNodesToFreeze;++i)
	        newop->scxRecordsSeen[i] = (SCXRecord<K,V> *) info->llxResults[i];
	    newop->state = SCXRecord<K,V>::STATE_INPROGRESS;
	    newop->allFrozen = false;
	    newop->field = field;
	    newop->numberOfNodes = (char) info->numberOfNodes;
	    newop->numberOfNodesToFreeze = (char) info->numberOfNodesToFreeze;
	    return newop;
	}

	void *llx_intxn_markingwr_infowr(const int tid, Node<K,V> *node,
	                                 Node<K,V> **retLeft, Node<K,V> **retRight)
	{
	    SCXRecord<K,V> *scx1 = node->scxRecord;
	    int state = (IS_VERSION_NUMBER(scx1) ? SCXRecord<K,V>::STATE_COMMITTED : scx1->state);
	    bool marked = node->marked;
	    SOFTWARE_BARRIER;
	    if (marked) {
	        return NULL;
	    } else {
	        if ((state & SCXRecord<K,V>::STATE_COMMITTED) || state & SCXRecord<K,V>::STATE_ABORTED) {
	            *retLeft = node->left;
	            *retRight = node->right;
	            if (*retLeft == NULL) return (void*) LLX_RETURN_IS_LEAF;
	            return scx1;
	        }
	    }
	    return NULL; // fail
	}

	void *llx(const int tid, Node<K,V> *node, Node<K,V> **retLeft, 
	          Node<K,V> **retRight)
	{
	    bst_retired_info info;
	    SCXRecord<K,V> *scx1 = node->scxRecord;
//	    IF_FAIL_TO_PROTECT_SCX(info, tid, scx1, &node->scxRecord, &node->marked)
//	        return NULL; // return and retry

	    int state = (IS_VERSION_NUMBER(scx1) ? SCXRecord<K,V>::STATE_COMMITTED : scx1->state);
	    SOFTWARE_BARRIER;       // prevent compiler from moving the read of marked before the read of state (no hw barrier needed on x86/64, since there is no read-read reordering)
	    bool marked = node->marked;
	    SOFTWARE_BARRIER;       // prevent compiler from moving the reads scx2=node->scxRecord or scx3=node->scxRecord before the read of marked. (no h/w barrier needed on x86/64 since there is no read-read reordering)
	    if ((state & SCXRecord<K,V>::STATE_COMMITTED && !marked) || state & SCXRecord<K,V>::STATE_ABORTED) {
	        SOFTWARE_BARRIER;       // prevent compiler from moving the reads scx2=node->scxRecord or scx3=node->scxRecord before the read of marked. (no h/w barrier needed on x86/64 since there is no read-read reordering)
	        *retLeft = node->left;
	        *retRight = node->right;
	        if (*retLeft == NULL) return (void *) LLX_RETURN_IS_LEAF;

	        SOFTWARE_BARRIER; // prevent compiler from moving the read of node->scxRecord before the read of left or right
	        SCXRecord<K,V> *scx2 = node->scxRecord;
	        if (scx1 == scx2) {
	            // on x86/64, we do not need any memory barrier here to prevent mutable fields of node from being moved before our read of scx1, because the hardware does not perform read-read reordering. on another platform, we would need to ensure no read from after this point is reordered before this point (technically, before the read that becomes scx1)...
	            return scx1;    // success
	        } else {
//				if (shmem->shouldHelp()) {
//				IF_FAIL_TO_PROTECT_SCX(info, tid, scx2, &node->scxRecord, &node->marked)
//					return NULL;
				assert(scx2 != dummy);
				if (!IS_VERSION_NUMBER(scx2)) help(tid, scx2, true);
//				}
	        }
	    } else if (state == SCXRecord<K,V>::STATE_INPROGRESS) {
//			if (shmem->shouldHelp()) {
			assert(scx1 != dummy);
			if (!IS_VERSION_NUMBER(scx1)) help(tid, scx1, true);
//			}
	    } else {
	        // state committed and marked
	        assert(state == 1); /* SCXRecord<K,V>::STATE_COMMITTED */
	        assert(marked);
//			if (shmem->shouldHelp()) {
			SCXRecord<K,V> *scx3 = node->scxRecord;
//			IF_FAIL_TO_PROTECT_SCX(info, tid, scx3, &node->scxRecord, &node->marked) 
//				return NULL;
			assert(scx3 != dummy);
			if (!IS_VERSION_NUMBER(scx3)) help(tid, scx3, true);
//			} else {
//			}
	    }
	    return NULL;            // fail
	}

	bool scx(const int tid, ReclamationInfo<K,V> * const info,
	         Node<K,V> * volatile * field, Node<K,V> * newNode)
	{
	    SCXRecord<K,V> *newscxrecord = allocateSCXRecord(tid);
	    initializeSCXRecord(tid, newscxrecord, info, field, newNode);
	    
	    SOFTWARE_BARRIER;
	    int state = help(tid, newscxrecord, false);
	    info->state = newscxrecord->state;
//	    reclaimMemoryAfterSCX(tid, info);
	    return state & SCXRecord<K,V>::STATE_COMMITTED;
	}

	int help(const int tid, SCXRecord<K,V> *scx, bool helpingOther) {
		assert(scx != dummy);
	    const int nNodes                        = scx->numberOfNodes;
	    const int nFreeze                       = scx->numberOfNodesToFreeze;
	    Node<K,V> ** const nodes                = scx->nodes;
	    SCXRecord<K,V> ** const scxRecordsSeen  = scx->scxRecordsSeen;
	    Node<K,V> * const newNode               = scx->newNode;

	    int __state = scx->state;
	    if (__state != SCXRecord<K,V>::STATE_INPROGRESS) return __state;

	    int freezeCount = 0;

	    for (int i=helpingOther; i<nFreeze; ++i) {
	        if (scxRecordsSeen[i] == LLX_RETURN_IS_LEAF) {
	            assert(i > 0); // nodes[0] cannot be a leaf...
	            continue; // do not freeze leaves
	        }
	        
	        bool successfulCAS = __sync_bool_compare_and_swap(&nodes[i]->scxRecord, scxRecordsSeen[i], scx); // MEMBAR ON X86/64
	        SCXRecord<K,V> * exp = nodes[i]->scxRecord;
	        if (!successfulCAS && exp != scx) { // if work was not done
	            if (scx->allFrozen) {
	                assert(scx->state == 1); /*STATE_COMMITTED*/
	                return SCXRecord<K,V>::STATE_COMMITTED; // success
	            } else {
	                if (i == 0) {
	                    assert(!helpingOther);
	                    scx->state = ABORT_STATE_INIT(0, 0);
	                    return ABORT_STATE_INIT(0, 0); // scx is aborted (but no one else will ever know)
	                } else {
	                    int expectedState = SCXRecord<K,V>::STATE_INPROGRESS;
	                    int newState = ABORT_STATE_INIT(i, freezeCount);
	                    bool success = __sync_bool_compare_and_swap(&scx->state, expectedState, newState); // MEMBAR ON X86/64
	                    assert(expectedState != 1); /* not committed */
	                    // note2: a regular write will not do, here, since two people can start helping, one can abort at i>0, then after a long time, the other can fail to CAS i=0, so they can get different i values.
	                    assert(scx->state & 2); /* SCXRecord<K,V>::STATE_ABORTED */
	                    // ABORTED THE SCX AFTER PERFORMING ONE OR MORE SUCCESSFUL FREEZING CASs
	                    if (success) {
	                        return newState;
	                    } else {
	                        return scx->state; // expectedState; // this has been overwritten by compare_exchange_strong with the value that caused the CAS to fail.
	                    }
	                }
	            }
	        } else {
	            ++freezeCount;
	            const int state_inprogress = SCXRecord<K,V>::STATE_INPROGRESS;
	            assert(exp == scx || IS_VERSION_NUMBER((uintptr_t) exp) || (exp->state != state_inprogress));
	        }
	    }
	    scx->allFrozen = true;
	    SOFTWARE_BARRIER;
	    for (int i=1; i<nFreeze; ++i) {
	        if (scxRecordsSeen[i] == LLX_RETURN_IS_LEAF) continue; // do not mark leaves
	        nodes[i]->marked = true; // finalize all but first node
	    }
	
	    // CAS in the new sub-tree (update CAS)
	    __sync_bool_compare_and_swap(scx->field, nodes[1], newNode);
	    assert(scx->state < 2); // not aborted
	    scx->state = SCXRecord<K,V>::STATE_COMMITTED;
	    
	    return SCXRecord<K,V>::STATE_COMMITTED; // success
	}


private:

	int total_paths, total_nodes, bst_violations;
	int min_path_len, max_path_len;

	void validate_rec(Node<K,V> *root, int _th)
	{
		if (!root) return;
	
		Node<K,V> *left = root->left;
		Node<K,V> *right = root->right;
	
		total_nodes++;
		_th++;
	
		/* BST violation? */
		if (left &&  left->key >= root->key) bst_violations++;
		if (right && right->key < root->key) bst_violations++;
	
		/* We found a path (a node with at least one NULL child). */
		if (!left || !right) {
			total_paths++;
			if (_th <= min_path_len) min_path_len = _th;
			if (_th >= max_path_len) max_path_len = _th;
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
	
		validate_rec(root->left->left, 0);
	
		check_bst = (bst_violations == 0);
	
		log_info("Validation:\n");
		log_info("=======================\n");
		log_info("  BST Violation: %s\n",
		         check_bst ? "No [OK]" : "Yes [ERROR]");
		log_info("  Tree size: %8d\n", total_nodes);
		log_info("  Total paths: %d\n", total_paths);
		log_info("  Min/max paths length: %d/%d\n", min_path_len, max_path_len);
		log_info("\n");
	
		return check_bst;
	}

};

#define BST_BROWN_TEMPL template<typename K, typename V>
#define BST_BROWN_FUNCT bst_brown<K,V>

BST_BROWN_TEMPL
bool BST_BROWN_FUNCT::contains(const int tid, const K& key)
{
	const V ret = lookup_helper(tid, key);
	return ret != this->NO_VALUE;
}

BST_BROWN_TEMPL
const std::pair<V,bool> BST_BROWN_FUNCT::find(const int tid, const K& key)
{
	const V ret = lookup_helper(tid, key);
	return std::pair<V,bool>(ret, ret != this->NO_VALUE);
}

BST_BROWN_TEMPL
int BST_BROWN_FUNCT::rangeQuery(const int tid, const K& lo, const K& hi,
                      std::vector<std::pair<K,V>> kv_pairs)
{
	return 0;
}

BST_BROWN_TEMPL
const V BST_BROWN_FUNCT::insert(const int tid, const K& key, const V& val)
{
	return insertIfAbsent(tid, key, val);
}

BST_BROWN_TEMPL
const V BST_BROWN_FUNCT::insertIfAbsent(const int tid, const K& key, const V& val)
{
	return insert_helper(tid, key, val);
}

BST_BROWN_TEMPL
const std::pair<V,bool> BST_BROWN_FUNCT::remove(const int tid, const K& key)
{
	return delete_helper(tid, key);
}

BST_BROWN_TEMPL
bool BST_BROWN_FUNCT::validate()
{
	return validate_helper();
}


//// note: this needs to go up to NUM_OF_NODES for marking
//template<class K, class V, class Compare, class RecManager>
//__rtm_force_inline bool bst<K,V,Compare,RecManager>::scx_htm(
//            const int tid,
//            ReclamationInfo<K,V> * const info,
//            Node<K,V> * volatile * field,        // pointer to a "field pointer" that will be changed
//            Node<K,V> * newNode) {
//    SCXRecord<K,V>* scx = (SCXRecord<K,V>*) NEXT_VERSION_NUMBER(tid);
//    const int n = info->numberOfNodesToFreeze;
//TXN1: int attempts = MAX_FAST_HTM_RETRIES;
//    int status = XBEGIN();
//    if (status == _XBEGIN_STARTED) {
//        // abort if someone on the fastHTM or fallback path
//        // changed nodes[i]->scxRecord since we last performed LLX on nodes[i].
//        switch (n) {
//            case 7: if (info->llxResults[6] != LLX_RETURN_IS_LEAF && info->nodes[6]->scxRecord != info->llxResults[6]) XABORT(ABORT_SCXRECORD_POINTER_CHANGED6);
//            case 6: if (info->llxResults[5] != LLX_RETURN_IS_LEAF && info->nodes[5]->scxRecord != info->llxResults[5]) XABORT(ABORT_SCXRECORD_POINTER_CHANGED5);
//            case 5: if (info->llxResults[4] != LLX_RETURN_IS_LEAF && info->nodes[4]->scxRecord != info->llxResults[4]) XABORT(ABORT_SCXRECORD_POINTER_CHANGED4);
//            case 4: if (info->llxResults[3] != LLX_RETURN_IS_LEAF && info->nodes[3]->scxRecord != info->llxResults[3]) XABORT(ABORT_SCXRECORD_POINTER_CHANGED3);
//            case 3: if (info->llxResults[2] != LLX_RETURN_IS_LEAF && info->nodes[2]->scxRecord != info->llxResults[2]) XABORT(ABORT_SCXRECORD_POINTER_CHANGED2);
//            case 2: if (info->llxResults[1] != LLX_RETURN_IS_LEAF && info->nodes[1]->scxRecord != info->llxResults[1]) XABORT(ABORT_SCXRECORD_POINTER_CHANGED1);
//            case 1: if (info->nodes[0]->scxRecord != info->llxResults[0]) XABORT(ABORT_SCXRECORD_POINTER_CHANGED0);
//        }
//        switch (n) {
//            case 7: info->nodes[6]->scxRecord = scx; info->nodes[6]->marked = true;
//            case 6: info->nodes[5]->scxRecord = scx; info->nodes[5]->marked = true;
//            case 5: info->nodes[4]->scxRecord = scx; info->nodes[4]->marked = true;
//            case 4: info->nodes[3]->scxRecord = scx; info->nodes[3]->marked = true;
//            case 3: info->nodes[2]->scxRecord = scx; info->nodes[2]->marked = true;
//            case 2: info->nodes[1]->scxRecord = scx; info->nodes[1]->marked = true;
//            case 1: info->nodes[0]->scxRecord = scx;
//        }
//        *field = newNode;
//        XEND();
//        info->state = SCXRecord<K,V>::STATE_COMMITTED;
//            this->counters->htmCommit[info->path]->inc(tid); //if (info->capacityAborted[info->path]) { this->counters->htmCapacityAbortThenCommit[info->path]->inc(tid); }
//        return true;
//    } else {
//        info->lastAbort = status;
//        IF_ALWAYS_RETRY_WHEN_BIT_SET if (status & _XABORT_RETRY) { this->counters->pathFail[info->path]->inc(tid); this->counters->htmRetryAbortRetried[info->path]->inc(tid); goto TXN1; }
//        return false;
//    }
//}
//
//
//template<class K, class V, class Compare, class RecManager>
//__rtm_force_inline void *bst<K,V,Compare,RecManager>::llx_htm(
//            const int tid,
//            Node<K,V> *node,
//            Node<K,V> **retLeft,
//            Node<K,V> **retRight) {
//    SCXRecord<K,V> *scx1 = node->scxRecord;
//    bool marked = node->marked;
//    int state = (IS_VERSION_NUMBER(scx1) ? SCXRecord<K,V>::STATE_COMMITTED : scx1->state);
//    SOFTWARE_BARRIER;       // prevent compiler from moving the read of marked before the read of state (no hw barrier needed on x86/64, since there is no read-read reordering)
//    if ((state & SCXRecord<K,V>::STATE_COMMITTED && !marked) || state & SCXRecord<K,V>::STATE_ABORTED) {
//        SOFTWARE_BARRIER;       // prevent compiler from moving the reads scx2=node->scxRecord or scx3=node->scxRecord before the read of marked. (no h/w barrier needed on x86/64 since there is no read-read reordering)
//        *retLeft = node->left;
//        *retRight = node->right;
//        if (*retLeft == NULL) {
//            TRACE COUTATOMICTID("llx return2.a (tid="<<tid<<" state="<<state<<" marked="<<marked<<" key="<<node->key<<")\n"); 
//            return (void*) LLX_RETURN_IS_LEAF;
//        }
//        SOFTWARE_BARRIER; // prevent compiler from moving the read of node->scxRecord before the read of left or right
//        SCXRecord<K,V> *scx2 = node->scxRecord;
//        if (scx1 == scx2) {
//            return scx1;
//        }
//    }
//    return NULL;           // fail
//}
