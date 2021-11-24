/**
 * An external (a-b) search tree with 3-path synchronization.
 * Paper:
 *    , Brown et. al, PPoPP 
 **/

#pragma once

#include <immintrin.h>
#include "../../map_if.h"
#include "Log.h"

using namespace std;

/* 2-bit state | 5-bit highest index reached | 24-bit frozen flags for each element of nodes[] on which a freezing CAS was performed = total 31 bits (highest bit unused) */
#define ABORT_STATE_INIT(i, flags) (abtree_SCXRecord<DEGREE,K>::STATE_ABORTED | ((i)<<2) | ((flags)<<7))
#define STATE_GET_FLAGS(state) ((state) & 0x7FFFFF80)
#define STATE_GET_HIGHEST_INDEX_REACHED(state) (((state) & 0x7C)>>2)
#define STATE_GET_WITH_FLAG_OFF(state, i) ((state) & ~(1<<(i+7)))

#define LLX_RETURN_IS_LEAF ((void*) 1)
#define MAX_TID_POW2 128

static const int MAX_NODES = 6;
static const int NUMBER_OF_PATHS = 3;
static const int PATH_FAST_HTM = 0;
static const int PATH_SLOW_HTM = 1;
static const int PATH_FALLBACK = 2;

static const int ABORT_PROCESS_ON_FALLBACK = 3;
static const int ABORT_UPDATE_FAILED = 2;
static const int ABORT_NODE_POINTER_CHANGED = 12;
static const int ABORT_LLX_FAILED = 13;

#define THREE_PATH_BEGIN(info) \
    info.path = (MAX_FAST_HTM_RETRIES >= 0 ? PATH_FAST_HTM : MAX_SLOW_HTM_RETRIES >= 0 ? PATH_SLOW_HTM : PATH_FALLBACK); \
    int attempts = 0; \
    for (;;) {

#define THREE_PATH_END(info, finished) \
        ++attempts; \
        if (finished) { \
            if ((info.path == PATH_FALLBACK) && (MAX_FAST_HTM_RETRIES >= 0 || MAX_SLOW_HTM_RETRIES >= 0)) { \
                __sync_fetch_and_add(&numFallback, -1); \
            } \
            break; \
        } \
        switch (info.path) { \
            case PATH_FAST_HTM: \
                /* check if we should change paths */ \
                if (attempts > MAX_FAST_HTM_RETRIES) { \
                    attempts = 0; \
                    if (MAX_SLOW_HTM_RETRIES < 0) { \
                        info.path = PATH_FALLBACK; \
                        __sync_fetch_and_add(&numFallback, 1); \
                    } else { \
                        info.path = PATH_SLOW_HTM; \
                    } \
                /* MOVE TO THE MIDDLE PATH IMMEDIATELY IF SOMEONE IS ON THE FALLBACK PATH */ \
                } else if ((info.lastAbort >> 24) == ABORT_PROCESS_ON_FALLBACK && MAX_SLOW_HTM_RETRIES >= 0) { \
                    attempts = 0; \
                    info.path = PATH_SLOW_HTM; \
                /* if there is no middle path, wait for the fallback path to be empty */ \
                } else if (MAX_SLOW_HTM_RETRIES < 0) { \
                    while (numFallback > 0) { __asm__ __volatile__("pause;"); } \
                } \
                break; \
            case PATH_SLOW_HTM: \
                /* check if we should change paths */ \
                if (attempts > MAX_SLOW_HTM_RETRIES) { \
                    attempts = 0; \
                    info.path = PATH_FALLBACK; \
                    __sync_fetch_and_add(&numFallback, 1); \
                } \
                break; \
            case PATH_FALLBACK: { \
                /** BEGIN DEBUG **/ \
                const int MAX_ATTEMPTS = 1000000; \
                if (attempts == MAX_ATTEMPTS) { std::cout<<"ERROR: more than "<<MAX_ATTEMPTS<<" attempts on fallback"<<std::endl; } \
                if (attempts > 2*MAX_ATTEMPTS) { std::cout<<"ERROR: more than "<<(2*MAX_ATTEMPTS)<<" attempts on fallback"<<std::endl; this->debugPrint(); exit(-1); } \
                /** END DEBUG **/ \
                } \
                break; \
            default: \
                std::cout<<"reached impossible switch case"<<std::endl; \
                exit(-1); \
                break; \
        } \
    }


template <typename K, typename V>
struct kvpair {
    K key;
    V val;
    kvpair() {}
};

template <typename K, typename V>
int kv_compare(const void * _a, const void * _b) {
    const kvpair<K,V> *a = (const kvpair<K,V> *) _a;
    const kvpair<K,V> *b = (const kvpair<K,V> *) _b;
    return (a->key < b->key) ? -1
         : (b->key < a->key) ? 1
         : 0;
}

static volatile char padding0[PREFETCH_SIZE_BYTES];
static long version[MAX_TID_POW2*PREFETCH_SIZE_WORDS];
static volatile char padding1[PREFETCH_SIZE_BYTES];
#define VERSION_NUMBER(tid) (version[(tid)*PREFETCH_SIZE_WORDS])
#define INIT_VERSION_NUMBER(tid) (VERSION_NUMBER(tid) = ((tid << 1) | 1))
#define NEXT_VERSION_NUMBER(tid) (VERSION_NUMBER(tid) += (MAX_TID_POW2 << 1))
#define IS_VERSION_NUMBER(infoPtr) (((long) (infoPtr)) & 1)

template <int DEGREE, typename K>
struct abtree_Node;

template <int DEGREE, typename K>
struct abtree_SCXRecord;

template <int DEGREE, typename K>
class wrapper_info {
public:
    const static int MAX_NODES = 4;
    abtree_Node<DEGREE,K> * nodes[MAX_NODES];
    abtree_SCXRecord<DEGREE,K> * scxRecordsSeen[MAX_NODES];
    abtree_Node<DEGREE,K> * newNode;
    void * volatile * field;
    int state;
    char numberOfNodes;
    char numberOfNodesToFreeze;
    char numberOfNodesAllocated;
    char path;
    int lastAbort;
    wrapper_info() {
        path = 0;
        state = 0;
        numberOfNodes = 0;
        numberOfNodesToFreeze = 0;
        numberOfNodesAllocated = 0;
        lastAbort = 0;
    }
};

template <int DEGREE, typename K>
struct abtree_SCXRecord {
    const static int STATE_INPROGRESS = 0;
    const static int STATE_COMMITTED = 1;
    const static int STATE_ABORTED = 2;

    volatile char numberOfNodes;
    volatile char numberOfNodesToFreeze;

    volatile int allFrozen;
    volatile int state; // state of the scx
    abtree_Node<DEGREE,K> * volatile newNode;
    void * volatile * field;
    abtree_Node<DEGREE,K> * volatile nodes[wrapper_info<DEGREE,K>::MAX_NODES];                // array of pointers to nodes
    abtree_SCXRecord<DEGREE,K> * volatile scxRecordsSeen[wrapper_info<DEGREE,K>::MAX_NODES];  // array of pointers to scx records
};

template <int DEGREE, typename K>
struct abtree_Node {
    abtree_SCXRecord<DEGREE,K> * volatile scxRecord;
    volatile int leaf;
    volatile int marked;
    volatile int tag;
    volatile int size; // number of keys; positive for internal, negative for leaves

    K keys[DEGREE];
    void * volatile ptrs[DEGREE];
    
    bool isLeaf() {
        return leaf;
    }
    int getKeyCount() {
//        TXN_ASSERT(size >= 0);
//        TXN_ASSERT(size <= DEGREE);
//        TXN_ASSERT(isLeaf() || size > 0);
        return isLeaf() ? size : size-1;
    }
    int getABDegree() {
        return size;
    }
    int getChildIndex(const K& key) {
        int nkeys = getKeyCount();
        int retval = 0;
        while (retval < nkeys && !(key < (const K&) keys[retval])) {
//            TXN_ASSERT(keys[retval] >= 0 && keys[retval] < MAXKEY);
            ++retval;
        }
        return retval;
    }
    abtree_Node<DEGREE,K> * getChild(const K& key) {
        return (abtree_Node<DEGREE,K> *) ptrs[getChildIndex(key)];
    }
    int getKeyIndex(const K& key) {
        int nkeys = getKeyCount();
        for (int i=0;i<nkeys;++i) {
//            TXN_ASSERT(keys[i] >= 0 && keys[i] < MAXKEY);
            if (!(key < (const K&) keys[i]) && !((const K&) keys[i] < key)) return i;
        }
        return nkeys;
    }
};


template <typename K, typename V, int DEGREE=16, int MIN_DEGREE=6>
class abtree_brown_3path : public Map<K,V> {

private:
	const int MAX_FAST_HTM_RETRIES;
	const int MAX_SLOW_HTM_RETRIES;

	abtree_Node<DEGREE,K> *root;
	char pad0[64];

	volatile int numFallback;
	char pad1[64];

	abtree_SCXRecord<DEGREE,K> * volatile dummy;

public:
	abtree_brown_3path(const K _NO_KEY, const V _NO_VALUE, const int numProcesses,
	                   const int fast_htm_retries = 10, const int slow_htm_retries = 10)
      : Map<K,V>(_NO_KEY, _NO_VALUE),
	    MAX_FAST_HTM_RETRIES(fast_htm_retries),
	    MAX_SLOW_HTM_RETRIES(slow_htm_retries)
	{
        cout<<"sizeof(abtree_Node<16,int>: " << sizeof(abtree_Node<16,int>) << endl;
        cout<<"MIN_DEGREE: " << MIN_DEGREE<<endl;
        cout<<"DEGREE: " << DEGREE<<endl;
        cout<<"NO_VALUE="<<(long long)this->NO_VALUE<<endl;
        if (MIN_DEGREE > DEGREE/2) {
            cout<<"ERROR: MIN DEGREE must be <= DEGREE/2."<<endl;
            exit(-1);
        }

        const int tid = 0;
        initThread(tid);
        dummy = allocateSCXRecord(tid);                                                                                                                                                            
        dummy->state = abtree_SCXRecord<DEGREE,K>::STATE_COMMITTED;

        abtree_Node<DEGREE,K> *rootleft = allocateNode(tid);
        rootleft->scxRecord = dummy;
        rootleft->marked = false;
        rootleft->tag = false;
        rootleft->size = 0;
        rootleft->leaf = true;

        root = allocateNode(tid);
        root->ptrs[0] = rootleft;
        root->scxRecord = dummy;
        root->marked = false;
        root->tag = false;
        root->size = 1;
        root->leaf = false;

        numFallback = 0;
	}

	void initThread(const int tid) {
		INIT_VERSION_NUMBER(tid);
	};
	void deinitThread(const int tid) {
	};

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
			return "(a-b)-tree Brown (LLX/SCX)";
		else
	   		return "(a-b)-tree Brown (3-path)";
	}

	void print() { /* print_helper(); */ };
//	unsigned long long size() { return size_rec(root) - 2; };

private:

	abtree_SCXRecord<DEGREE,K> *allocateSCXRecord(const int tid)
	{
	    abtree_SCXRecord<DEGREE,K> *newop = new abtree_SCXRecord<DEGREE,K>();
	    if (newop == NULL) {
			std::cerr << "ERROR: could not allocate scx record\n";
	        exit(-1);
	    }
	    return newop;
	}

	abtree_Node<DEGREE,K> *allocateNode(const int tid)
	{
	    abtree_Node<DEGREE,K> *newnode = new abtree_Node<DEGREE,K>();
	    if (newnode == NULL) {
			std::cerr << "ERROR: could not allocate node\n";
	        exit(-1);
	    }
	    return newnode;
	}

	abtree_SCXRecord<DEGREE,K> *createSCXRecord(const int tid,
	              void * volatile * const field, abtree_Node<DEGREE,K> * const newNode,
	              abtree_Node<DEGREE,K> ** const nodes,
	              abtree_SCXRecord<DEGREE,K> ** const scxRecordsSeen,
	              const int numberOfNodes, const int numberOfNodesToFreeze)
	{
	    abtree_SCXRecord<DEGREE,K> *result = new abtree_SCXRecord<DEGREE,K>();
	    result->allFrozen = false;
	    result->field = field;
	    result->newNode = newNode;
	    result->state = abtree_SCXRecord<DEGREE,K>::STATE_INPROGRESS;
	    for (int i=0;i<numberOfNodes;++i)
			result->nodes[i] = nodes[i];
	    for (int i=0;i<numberOfNodes;++i)
	        result->scxRecordsSeen[i] = scxRecordsSeen[i];
	    result->numberOfNodes = numberOfNodes;
	    result->numberOfNodesToFreeze = numberOfNodesToFreeze;
	    return result;
	}

	bool isSentinel(abtree_Node<DEGREE,K> * node) {
	    return (node == root || node == root->ptrs[0]);
	}

	long long debugKeySum(abtree_Node<DEGREE,K> * node)
	{
	    long long sum = 0;
	    if (node == NULL) return 0;
	    if (node->isLeaf()) {
	        for (int i=0;i<node->getABDegree();++i)
	            sum += node->keys[i];
	    } else {
	        for (int i=0;i<node->getABDegree();++i)
	            sum += debugKeySum((abtree_Node<DEGREE,K> *) node->ptrs[i]);
	    }
	    return sum;
	}

	const V lookup_helper(const int tid, const K& key)
	{
	    V result;
	    abtree_Node<DEGREE,K> * l = (abtree_Node<DEGREE,K> *) root->ptrs[0];
	
	    while (!l->isLeaf()) l = l->getChild(key);
	
	    int index = l->getKeyIndex(key);
	    if (index < l->getKeyCount()) return (V)l->ptrs[index];
	    else                          return this->NO_VALUE;
	}

	const V insert_helper(const int tid, const K& key, const V& val, bool onlyIfAbsent)
	{
	    bool shouldRebalance = false;
	    V result = this->NO_VALUE;
	    
	    // do insert
	    wrapper_info<DEGREE,K> info;
	    bool retval = false;
	    THREE_PATH_BEGIN(info);
	        if (info.path == PATH_FAST_HTM) {
	            retval = insert_fast(&info, tid, key, val, onlyIfAbsent, &shouldRebalance, &result);
	        } else if (info.path == PATH_SLOW_HTM) {
	            retval = insert_middle(&info, tid, key, val, onlyIfAbsent, &shouldRebalance, &result);
	        } else {
	            retval = insert_fallback(&info, tid, key, val, onlyIfAbsent, &shouldRebalance, &result);
	        }
	    THREE_PATH_END(info, retval);
	
	    // do rebalancing
	    while (shouldRebalance) {
	        info = wrapper_info<DEGREE,K>();
	        THREE_PATH_BEGIN(info);
	            if (info.path == PATH_FAST_HTM) {
	                retval = rebalance_fast(&info, tid, key, &shouldRebalance);
	            } else if (info.path == PATH_SLOW_HTM) {
	                retval = rebalance_middle(&info, tid, key, &shouldRebalance);
	            } else {
	                retval = rebalance_fallback(&info, tid, key, &shouldRebalance);
	            }
	        THREE_PATH_END(info, retval);
	    }
	    return result;
	}

	const V delete_helper(const int tid, const K& key)
	{
	    bool shouldRebalance = false;
	    V result = this->NO_VALUE;
	    
	    // do insert
	    wrapper_info<DEGREE,K> info;
	    bool retval = false;
	    THREE_PATH_BEGIN(info);
	        if (info.path == PATH_FAST_HTM) {
	            retval = erase_fast(&info, tid, key, result, &shouldRebalance);
	        } else if (info.path == PATH_SLOW_HTM) {
	            retval = erase_middle(&info, tid, key, result, &shouldRebalance);
	        } else {
	            retval = erase_fallback(&info, tid, key, result, &shouldRebalance);
	        }
	    THREE_PATH_END(info, retval);
	
	    // do rebalancing
	    while (shouldRebalance) {
	        info = wrapper_info<DEGREE,K>();
	        THREE_PATH_BEGIN(info);
	            if (info.path == PATH_FAST_HTM) {
	                retval = rebalance_fast(&info, tid, key, &shouldRebalance);
	            } else if (info.path == PATH_SLOW_HTM) {
	                retval = rebalance_middle(&info, tid, key, &shouldRebalance);
	            } else {
	                retval = rebalance_fallback(&info, tid, key, &shouldRebalance);
	            }
	        THREE_PATH_END(info, retval);
	    }
	    return result;
	}

	bool insert_fast(wrapper_info<DEGREE,K> * const info, const int tid, const K& key, const V& val, const bool onlyIfAbsent, bool * const shouldRebalance, V * const result)
	{
	    abtree_Node<DEGREE,K> * p;
	    abtree_Node<DEGREE,K> * l;
	    int keyindexl;
	    int lindex;
	    int nkeysl;
	    bool found;
	    abtree_Node<DEGREE,K> * parent;
	    abtree_Node<DEGREE,K> * left;
	    abtree_Node<DEGREE,K> * right;
	    kvpair<K,V> tosort[DEGREE+1];
	
	    int attempts = MAX_FAST_HTM_RETRIES;
	TXN1: (0);
	    int status = _xbegin();
	    if (status == _XBEGIN_STARTED) {
	        if (numFallback > 0) 
				_xabort(ABORT_PROCESS_ON_FALLBACK);
	
	        p = root;
	        l = (abtree_Node<DEGREE,K> *) root->ptrs[0];
	//        TXN_ASSERT(l);
	        while (!l->isLeaf()) {
	            p = l;
	            l = l->getChild(key);
	//            TXN_ASSERT(l);
	        }
	        keyindexl = l->getKeyIndex(key);
	        lindex = p->getChildIndex(key);
	        nkeysl = l->getKeyCount();
	   
	        found = (keyindexl < nkeysl);
	        if (found) {
	            if (!onlyIfAbsent) l->ptrs[keyindexl] = val;
                *result = (V)l->ptrs[keyindexl];
                _xend();
                *shouldRebalance = false;
                return true;
	        } else {
	            if (nkeysl < DEGREE) {
	                // inserting new key/value pair into leaf
	                l->keys[nkeysl] = key;
	                l->ptrs[nkeysl] = val;
	                ++l->size;
	//                __sync_synchronize();
	                *result = this->NO_VALUE;
	                _xend();
	                *shouldRebalance = false;
	                return true;
	            } else { // nkeysl == DEGREE
	                // overflow: insert a new tagged parent above l and create a new sibling
	                right = l;
	
	                for (int i=0;i<nkeysl;++i) {
	                    tosort[i].key = l->keys[i];
	                    tosort[i].val = (V)l->ptrs[i];
	                }
	                tosort[nkeysl].key = key;
	                tosort[nkeysl].val = val;
	                qsort(tosort, nkeysl+1, sizeof(kvpair<K,V>), kv_compare<K,V>);
	
					parent = new abtree_Node<DEGREE, K>();
					parent->scxRecord = dummy;
					parent->marked = false;
					left = new abtree_Node<DEGREE, K>();
					left->scxRecord = dummy;
					left->marked = false;

	                const int leftLength = (nkeysl+1)/2;
	                for (int i=0;i<leftLength;++i) {
	                    left->keys[i] = tosort[i].key;
	                }
	                for (int i=0;i<leftLength;++i) {
	                    left->ptrs[i] = tosort[i].val;
	                }
	                left->tag = false;
	                left->size = leftLength;
	                left->leaf = true;
	
	                const int rightLength = (nkeysl+1) - leftLength;
	                for (int i=0;i<rightLength;++i) {
	                    right->keys[i] = tosort[i+leftLength].key;
	                }
	                for (int i=0;i<rightLength;++i) {
	                    right->ptrs[i] = tosort[i+leftLength].val;
	                }
	                right->size = rightLength;
	
	                parent->keys[0] = right->keys[0];
	                parent->ptrs[0] = left;
	                parent->ptrs[1] = right;
	                parent->tag = (p != root);
	                parent->size = 2;
	                parent->leaf = false;
	                
	                p->ptrs[lindex] = parent;
	                *result = this->NO_VALUE;
	                _xend();
	                
	                *shouldRebalance = true;
	                
	                // do memory reclamation and allocation
	//                REPLACE_ALLOCATED_NODE(tid, 0);
	//                REPLACE_ALLOCATED_NODE(tid, 1);
	                
	                return true;
	            }
	        }
	    } else {
	aborthere:
	        if (info) info->lastAbort = status;
	//        IF_ALWAYS_RETRY_WHEN_BIT_SET if (status & _XABORT_RETRY) { this->counters->pathFail[info->path]->inc(tid); this->counters->htmRetryAbortRetried[info->path]->inc(tid); goto TXN1; }
	        return false;
	    }
	}

	bool insert_middle(wrapper_info<DEGREE,K> * const info, const int tid, const K& key, const V& val, const bool onlyIfAbsent, bool * const shouldRebalance, V * const result)
	{
	tXN1: int attempts = MAX_FAST_HTM_RETRIES;
	    int status = _xbegin();
	    if (status == _XBEGIN_STARTED) {
	        abtree_Node<DEGREE,K> * p = root;
	        abtree_Node<DEGREE,K> * l = (abtree_Node<DEGREE,K> *) root->ptrs[0];
	        while (!l->isLeaf()) {
	//            TXN_ASSERT(l->ptrs[0] != NULL);
	            p = l;
	            l = l->getChild(key);
	        }
	
	        void * ptrsp[DEGREE];
	        if ((info->scxRecordsSeen[0] = (abtree_SCXRecord<DEGREE,K> *) llx_txn(tid, p, ptrsp)) == NULL)
				_xabort(ABORT_LLX_FAILED);
	        int lindex = p->getChildIndex(key);
	        if (p->ptrs[lindex] != l)
				_xabort(ABORT_NODE_POINTER_CHANGED);
	        info->nodes[0] = p;
	
	        void * ptrsl[DEGREE];
	        if ((info->scxRecordsSeen[1] = (abtree_SCXRecord<DEGREE,K> *) llx_txn(tid, l, ptrsl)) == NULL)
				_xabort(ABORT_LLX_FAILED);
	        int keyindexl = l->getKeyIndex(key);
	        info->nodes[1] = l;
	
	        int nkeysl = l->getKeyCount();
	        bool found = (keyindexl < nkeysl);
	        if (found) {
	            if (onlyIfAbsent) {
	                *result = (V)l->ptrs[keyindexl];
	                _xend();
	                *shouldRebalance = false;
	                return true;
	            } else {
	                // replace leaf with a new copy that has the new value instead of the old one
	                abtree_Node<DEGREE,K> * newNode = new abtree_Node<DEGREE,K>();
	                newNode->marked = false;
	                newNode->scxRecord = dummy;
	
	                for (int i=0;i<nkeysl;++i) {
	                    newNode->keys[i] = l->keys[i];
	                }
	                for (int i=0;i<nkeysl;++i) {
	                    newNode->ptrs[i] = l->ptrs[i];
	                }
	                newNode->ptrs[keyindexl] = val;
	
	                newNode->tag = false;
	                newNode->leaf = true;
	                newNode->size = l->size;
	
	                info->numberOfNodesAllocated = 1;
	                info->numberOfNodesToFreeze = 2;
	                info->numberOfNodes = 2;
	                info->field = &p->ptrs[lindex];
	                info->newNode = newNode;
	
	                *result = (V)l->ptrs[keyindexl];
	                *shouldRebalance = false;
	            }
	        } else {
	            if (nkeysl < DEGREE) {
	                // replace leaf with a new copy that has the new key/value pair inserted
	                abtree_Node<DEGREE,K> * newNode = new abtree_Node<DEGREE,K>();
	                newNode->marked = false;
	                newNode->scxRecord = dummy;
	
	                for (int i=0;i<nkeysl;++i) {
	                    newNode->keys[i] = l->keys[i];
	                }
	                for (int i=0;i<nkeysl;++i) {
	                    newNode->ptrs[i] = l->ptrs[i];
	                }
	                newNode->keys[nkeysl] = key;
	                newNode->ptrs[nkeysl] = val;
	
	                newNode->tag = false;
	                newNode->leaf = true;
	                newNode->size = nkeysl+1;
	
	                info->numberOfNodesAllocated = 1;
	                info->numberOfNodesToFreeze = 2;
	                info->numberOfNodes = 2;
	                info->field = &p->ptrs[lindex];
	                info->newNode = newNode;
	
	                *result = this->NO_VALUE;
	                *shouldRebalance = false;
	            } else { // nkeysl == DEGREE
	                // overflow: replace the leaf with a subtree of three nodes
	                abtree_Node<DEGREE,K> * parent = new abtree_Node<DEGREE,K>();
	                parent->marked = false;
	                parent->scxRecord = dummy;
	
	                abtree_Node<DEGREE,K> * left = new abtree_Node<DEGREE,K>();
	                left->marked = false;
	                left->scxRecord = dummy;
	
	                abtree_Node<DEGREE,K> * right = new abtree_Node<DEGREE,K>();
	                right->marked = false;
	                right->scxRecord = dummy;
	
	                kvpair<K,V> tosort[nkeysl+1];
	                for (int i=0;i<nkeysl;++i) {
	                    tosort[i].key = l->keys[i];
	                    tosort[i].val = (V)l->ptrs[i];
	                }
	                tosort[nkeysl].key = key;
	                tosort[nkeysl].val = val;
	                qsort(tosort, nkeysl+1, sizeof(kvpair<K,V>), kv_compare<K,V>);
	
	                const int leftLength = (nkeysl+1)/2;
	                for (int i=0;i<leftLength;++i) {
	                    left->keys[i] = tosort[i].key;
	                }
	                for (int i=0;i<leftLength;++i) {
	                    left->ptrs[i] = tosort[i].val;
	                }
	                left->tag = false;
	                left->size = leftLength;
	                left->leaf = true;
	
	                const int rightLength = (nkeysl+1) - leftLength;
	                for (int i=0;i<rightLength;++i) {
	                    right->keys[i] = tosort[i+leftLength].key;
	                }
	                for (int i=0;i<rightLength;++i) {
	                    right->ptrs[i] = tosort[i+leftLength].val;
	                }
	                right->tag = false;
	                right->size = rightLength;
	                right->leaf = true;
	
	                parent->keys[0] = right->keys[0];
	                parent->ptrs[0] = left;
	                parent->ptrs[1] = right;
	                parent->tag = (p != root);
	                parent->size = 2;
	                parent->leaf = false;
	
	                info->numberOfNodesAllocated = 3;
	                info->numberOfNodesToFreeze = 2;
	                info->numberOfNodes = 2;
	                info->field = &p->ptrs[lindex];
	                info->newNode = parent;
	
	                *result = this->NO_VALUE;
	                *shouldRebalance = true;
	            }
	        }
	
	        bool retval = scx_txn(tid, info);
	        _xend();
	//        reclaimMemoryAfterSCX(tid, info, true);
			return retval;
	    } else {
	aborthere:
	        if (info) info->lastAbort = status;
	//        IF_ALWAYS_RETRY_WHEN_BIT_SET if (status & _XABORT_RETRY) { this->counters->pathFail[info->path]->inc(tid); this->counters->htmRetryAbortRetried[info->path]->inc(tid); goto TXN1; }
	        return false;
	    }
	}

	bool insert_fallback(wrapper_info<DEGREE,K> * const info, const int tid, const K& key, const V& val, const bool onlyIfAbsent, bool * const shouldRebalance, V * const result)
	{
	    abtree_Node<DEGREE,K> * p = root;
	    abtree_Node<DEGREE,K> * l = (abtree_Node<DEGREE,K> *) root->ptrs[0];
	    while (!l->isLeaf()) {
	        p = l;
	        l = l->getChild(key);
	    }
	    
	    void * ptrsp[DEGREE];
	    if ((info->scxRecordsSeen[0] = (abtree_SCXRecord<DEGREE,K> *) llx(tid, p, ptrsp)) == NULL) 
			return false;
	    int lindex = p->getChildIndex(key);
	    if (p->ptrs[lindex] != l) return false;
	    info->nodes[0] = p;
	    
	    void * ptrsl[DEGREE];
	    if ((info->scxRecordsSeen[1] = (abtree_SCXRecord<DEGREE,K> *) llx(tid, l, ptrsl)) == NULL)
			return false;
	    int keyindexl = l->getKeyIndex(key);
	    info->nodes[1] = l;
	    
	    int nkeysl = l->getKeyCount();
	    bool found = (keyindexl < nkeysl);
	    if (found) {
	        if (onlyIfAbsent) {
	            *result = (V)l->ptrs[keyindexl];
	            *shouldRebalance = false;
	            return true;
	        } else {
	            // replace leaf with a new copy that has the new value instead of the old one
	            abtree_Node<DEGREE,K> * newNode = new abtree_Node<DEGREE,K>();
	            newNode->marked = false;
	            newNode->scxRecord = dummy;
	
	            for (int i=0;i<nkeysl;++i) {
	                newNode->keys[i] = l->keys[i];
	            }
	            for (int i=0;i<nkeysl;++i) {
	                newNode->ptrs[i] = l->ptrs[i];
	            }
	            newNode->ptrs[keyindexl] = val;
	
	            newNode->tag = false;
	            newNode->leaf = true;
	            newNode->size = l->size;
	            
	            info->numberOfNodesAllocated = 1;
	            info->numberOfNodesToFreeze = 2;
	            info->numberOfNodes = 2;
	            info->field = &p->ptrs[lindex];
	            info->newNode = newNode;
	            
	            *result = (V)l->ptrs[keyindexl];
	            *shouldRebalance = false;
	        }
	    } else {
	        if (nkeysl < DEGREE) {
	            // replace leaf with a new copy that has the new key/value pair inserted
	            abtree_Node<DEGREE,K> * newNode = new abtree_Node<DEGREE,K>();
	            newNode->marked = false;
	            newNode->scxRecord = dummy;
	
	            for (int i=0;i<nkeysl;++i) {
	                newNode->keys[i] = l->keys[i];
	            }
	            for (int i=0;i<nkeysl;++i) {
	                newNode->ptrs[i] = l->ptrs[i];
	            }
	            newNode->keys[nkeysl] = key;
	            newNode->ptrs[nkeysl] = val;
	
	            newNode->tag = false;
	            newNode->leaf = true;
	            newNode->size = nkeysl+1;
	            
	            info->numberOfNodesAllocated = 1;
	            info->numberOfNodesToFreeze = 2;
	            info->numberOfNodes = 2;
	            info->field = &p->ptrs[lindex];
	            info->newNode = newNode;
	            
	            *result = this->NO_VALUE;
	            *shouldRebalance = false;
	        } else { // nkeysl == DEGREE
	            // overflow: replace the leaf with a subtree of three nodes
	            abtree_Node<DEGREE,K> * parent = new abtree_Node<DEGREE,K>();
	            parent->marked = false;
	            parent->scxRecord = dummy;
	
	            abtree_Node<DEGREE,K> * left = new abtree_Node<DEGREE,K>();
	            left->marked = false;
	            left->scxRecord = dummy;
	
	            abtree_Node<DEGREE,K> * right = new abtree_Node<DEGREE,K>();
	            right->marked = false;
	            right->scxRecord = dummy;
	
	            kvpair<K,V> tosort[nkeysl+1];
	            for (int i=0;i<nkeysl;++i) {
	                tosort[i].key = l->keys[i];
	                tosort[i].val = (V)l->ptrs[i];
	            }
	            tosort[nkeysl].key = key;
	            tosort[nkeysl].val = val;
	            qsort(tosort, nkeysl+1, sizeof(kvpair<K,V>), kv_compare<K,V>);
	            
	            const int leftLength = (nkeysl+1)/2;
	            for (int i=0;i<leftLength;++i) {
	                left->keys[i] = tosort[i].key;
	            }
	            for (int i=0;i<leftLength;++i) {
	                left->ptrs[i] = tosort[i].val;
	            }
	            left->tag = false;
	            left->size = leftLength;
	            left->leaf = true;
	            
	            const int rightLength = (nkeysl+1) - leftLength;
	            for (int i=0;i<rightLength;++i) {
	                right->keys[i] = tosort[i+leftLength].key;
	            }
	            for (int i=0;i<rightLength;++i) {
	                right->ptrs[i] = tosort[i+leftLength].val;
	            }
	            right->tag = false;
	            right->size = rightLength;
	            right->leaf = true;
	            
	            parent->keys[0] = right->keys[0];
	            parent->ptrs[0] = left;
	            parent->ptrs[1] = right;
	            parent->tag = (p != root);
	            parent->size = 2;
	            parent->leaf = false;
	            
	            info->numberOfNodesAllocated = 3;
	            info->numberOfNodesToFreeze = 2;
	            info->numberOfNodes = 2;
	            info->field = &p->ptrs[lindex];
	            info->newNode = parent;
	            
	            *result = this->NO_VALUE;
	            *shouldRebalance = true;
	        }
	    }
	    
	    bool retval = scx(tid, info);
//	    reclaimMemoryAfterSCX(tid, info, false);
		return retval;
	}

	bool erase_fast(wrapper_info<DEGREE,K> * const info, const int tid,
	                const K& key, V& val, bool * const shouldRebalance)
	{
	    abtree_Node<DEGREE,K> * p;
	    abtree_Node<DEGREE,K> * l;
	    int keyindexl;
	    int lindex;
	    int nkeysl;
	    bool found;
	    kvpair<K,V> tosort[DEGREE+1];
	
	    int attempts = MAX_FAST_HTM_RETRIES;
	TXN1: (0);
	    int status = _xbegin();
	    if (status == _XBEGIN_STARTED) {
	        if (numFallback > 0) 
				_xabort(ABORT_PROCESS_ON_FALLBACK);
	
	        p = root;
	        l = (abtree_Node<DEGREE,K> *) root->ptrs[0];
	//        TXN_ASSERT(l);
	        while (!l->isLeaf()) {
	            p = l;
	            l = l->getChild(key);
	//            TXN_ASSERT(l);
	        }
	        keyindexl = l->getKeyIndex(key);
	        lindex = p->getChildIndex(key);
	        nkeysl = l->getKeyCount();
	   
	        found = (keyindexl < nkeysl);
	        if (found) {
				//> Delete
	            val = (V)l->ptrs[keyindexl];
	            l->keys[keyindexl] = l->keys[nkeysl-1];
	            l->ptrs[keyindexl] = l->ptrs[nkeysl-1];
	            --l->size;
	            *shouldRebalance = (nkeysl < MIN_DEGREE);
	        } else {
				val = this->NO_VALUE;
				*shouldRebalance = false;
	        }
            _xend();
			return true;
	    } else {
	aborthere:
	        if (info) info->lastAbort = status;
	//        IF_ALWAYS_RETRY_WHEN_BIT_SET if (status & _XABORT_RETRY) { this->counters->pathFail[info->path]->inc(tid); this->counters->htmRetryAbortRetried[info->path]->inc(tid); goto TXN1; }
	        return false;
	    }
	}

	bool erase_middle(wrapper_info<DEGREE,K> * const info, const int tid,
	                  const K& key, V& val, bool * const shouldRebalance)
	{
	TXN1: int attempts = MAX_FAST_HTM_RETRIES;
	    int status = _xbegin();
	    if (status == _XBEGIN_STARTED) {
	        abtree_Node<DEGREE,K> * p = root;
	        abtree_Node<DEGREE,K> * l = (abtree_Node<DEGREE,K> *) root->ptrs[0];
	        while (!l->isLeaf()) {
	//            TXN_ASSERT(l->ptrs[0] != NULL);
	            p = l;
	            l = l->getChild(key);
	        }
	
	        void * ptrsp[DEGREE];
	        if ((info->scxRecordsSeen[0] = (abtree_SCXRecord<DEGREE,K> *) llx_txn(tid, p, ptrsp)) == NULL) 
				_xabort(ABORT_LLX_FAILED);
	        int lindex = p->getChildIndex(key);
	        if (p->ptrs[lindex] != l)
				_xabort(ABORT_NODE_POINTER_CHANGED);
	        info->nodes[0] = p;
	
	        void * ptrsl[DEGREE];
	        if ((info->scxRecordsSeen[1] = (abtree_SCXRecord<DEGREE,K> *) llx_txn(tid, l, ptrsl)) == NULL) 
				_xabort(ABORT_LLX_FAILED);
	        int keyindexl = l->getKeyIndex(key);
	        info->nodes[1] = l;
	
	        int nkeysl = l->getKeyCount();
	        bool found = (keyindexl < nkeysl);
	        if (found) {
				//> Delete
				val = (V)l->ptrs[keyindexl];
	
	            // replace leaf with a new copy that has the appropriate key/value pair deleted
				abtree_Node<DEGREE,K> * newNode = new abtree_Node<DEGREE,K>();
	            newNode->scxRecord = dummy;
	            newNode->marked = false;
	
	            for (int i=0;i<keyindexl;++i) {
	                newNode->keys[i] = l->keys[i];
	            }
	            for (int i=0;i<keyindexl;++i) {
	                newNode->ptrs[i] = l->ptrs[i];
	            }
	            for (int i=keyindexl+1;i<nkeysl;++i) {
	                newNode->keys[i-1] = l->keys[i];
	            }
	            for (int i=keyindexl+1;i<nkeysl;++i) {
	                newNode->ptrs[i-1] = l->ptrs[i];
	            }
	            newNode->tag = false;
	            newNode->size = nkeysl-1;
	            newNode->leaf = true;
	
	            info->numberOfNodesAllocated = 1;
	            info->numberOfNodesToFreeze = 2;
	            info->numberOfNodes = 2;
	            info->field = &p->ptrs[lindex];
	            info->newNode = newNode;
	
	            *shouldRebalance = (nkeysl < MIN_DEGREE);
	        	bool retval = scx_txn(tid, info);
	        	_xend();
				return retval;
	        } else {
				*shouldRebalance = false;
				val = this->NO_VALUE;
	        	_xend();
				return true;
	        }
	    } else {
	aborthere:
	        if (info) info->lastAbort = status;
	//        IF_ALWAYS_RETRY_WHEN_BIT_SET if (status & _XABORT_RETRY) { this->counters->pathFail[info->path]->inc(tid); this->counters->htmRetryAbortRetried[info->path]->inc(tid); goto TXN1; }
	        return false;
	    }
	}

	bool erase_fallback(wrapper_info<DEGREE,K> * const info, const int tid,
	                    const K& key, V& val, bool * const shouldRebalance)
	{
	    abtree_Node<DEGREE,K> * p = root;
	    abtree_Node<DEGREE,K> * l = (abtree_Node<DEGREE,K> *) root->ptrs[0];
	    while (!l->isLeaf()) {
	        p = l;
	        l = l->getChild(key);
	    }
	    
	    void * ptrsp[DEGREE];
	    if ((info->scxRecordsSeen[0] = (abtree_SCXRecord<DEGREE,K> *) llx(tid, p, ptrsp)) == NULL)
			return false;
	    int lindex = p->getChildIndex(key);
	    if (p->ptrs[lindex] != l) 
			return false;
	    info->nodes[0] = p;
	    
	    void * ptrsl[DEGREE];
	    if ((info->scxRecordsSeen[1] = (abtree_SCXRecord<DEGREE,K> *) llx(tid, l, ptrsl)) == NULL)
			return false;
	    int keyindexl = l->getKeyIndex(key);
	    info->nodes[1] = l;
	    
	    int nkeysl = l->getKeyCount();
	    bool found = (keyindexl < nkeysl);
	    if (found) {
			//> Delete
			val = (V)l->ptrs[keyindexl];
	        // replace leaf with a new copy that has the appropriate key/value pair deleted
	        abtree_Node<DEGREE,K> * newNode = new abtree_Node<DEGREE,K>();
	        newNode->scxRecord = dummy;
	        newNode->marked = false;
	        
	        for (int i=0;i<keyindexl;++i) {
	            newNode->keys[i] = l->keys[i];
	        }
	        for (int i=0;i<keyindexl;++i) {
	            newNode->ptrs[i] = l->ptrs[i];
	        }
	        for (int i=keyindexl+1;i<nkeysl;++i) {
	            newNode->keys[i-1] = l->keys[i];
	        }
	        for (int i=keyindexl+1;i<nkeysl;++i) {
	            newNode->ptrs[i-1] = l->ptrs[i];
	        }
	        newNode->tag = false;
	        newNode->size = nkeysl-1;
	        newNode->leaf = true;
	        
	        info->numberOfNodesAllocated = 1;
	        info->numberOfNodesToFreeze = 2;
	        info->numberOfNodes = 2;
	        info->field = &p->ptrs[lindex];
	        info->newNode = newNode;
	
	        *shouldRebalance = (nkeysl < MIN_DEGREE);
            bool retval = scx(tid, info);
//            reclaimMemoryAfterSCX(tid, info, false);
	        return retval;
	    } else {
			*shouldRebalance = false;
			val = this->NO_VALUE;
			return true;
	    }
		assert(0);
	}

private:

	bool rebalance_fast(wrapper_info<DEGREE,K> * const info, const int tid, const K& key, bool * const shouldRebalance)
	{
	    // try to push into the next stack PAGE before starting the txn if we might encounter a page boundary
//	    char stackptr = 0;
//	    int rem = pagesize - (((long long) &stackptr) & pagesize);
//	    const int estNeeded = 2000; // estimate on the space needed for stack frames of all callees beyond this point
//	    if (rem < estNeeded) {
//	        volatile char x[rem+8];
//	        x[rem+7] = 0; // force page to load
//	    }
	    
	TXN1: int attempts = MAX_FAST_HTM_RETRIES;
	    int status = _xbegin();
	    if (status == _XBEGIN_STARTED) {
	        if (numFallback > 0) { _xabort(ABORT_PROCESS_ON_FALLBACK); }
	        abtree_Node<DEGREE,K> * gp = root;
	        abtree_Node<DEGREE,K> * p = root;
	        abtree_Node<DEGREE,K> * l = (abtree_Node<DEGREE,K> *) root->ptrs[0];
	
	        // Unrolled special case for root:
	        // Note: l is NOT tagged, but it might have only 1 child pointer, which would be a problem
	        if (l->isLeaf()) {
	            *shouldRebalance = false; // no violations to fix
	            _xend();
	            return true; // nothing can be wrong with the root, if it is a leaf
	        }
	        bool scxRecordReady = false;
	        if (l->getABDegree() == 1) { // root is internal and has only one child
	            if (!rootJoinParent_fast(info, tid, p, l, p->getChildIndex(key))) _xabort(ABORT_UPDATE_FAILED);
	            return true;
	        }
	
	        // root is internal, and there is nothing wrong with it, so move on
	        gp = p;
	        p = l;
	        l = l->getChild(key);
	
	        // check each subsequent node for tag violations and degree violations
	        while (!(l->isLeaf() || l->tag || (l->getABDegree() < MIN_DEGREE && !isSentinel(l)))) {
	            gp = p;
	            p = l;
	            l = l->getChild(key);
	        }
	
	        if (!(l->tag || (l->getABDegree() < MIN_DEGREE && !isSentinel(l)))) {
	            *shouldRebalance = false; // no violations to fix
	            _xend();
	            return true;
	        }
	
	        // tag operations take precedence
	        if (l->tag) {
	            if (p->getABDegree() + l->getABDegree() <= DEGREE+1) {
	                if (!tagJoinParent_fast(info, tid, gp, p, gp->getChildIndex(key), l, p->getChildIndex(key))) _xabort(ABORT_UPDATE_FAILED);
	                return true;
	            } else {
	                if (!tagSplit_fast(info, tid, gp, p, gp->getChildIndex(key), l, p->getChildIndex(key))) _xabort(ABORT_UPDATE_FAILED);
	                return true;
	            }
	        } else { // assert (l->getABDegree() < MIN_DEGREE)
	            // get sibling of l
	            abtree_Node<DEGREE,K> * s;
	            int lindex = p->getChildIndex(key);
	            //if (p->ptrs[lindex] != l) return false;
	            int sindex = lindex ? lindex-1 : lindex+1;
	            s = (abtree_Node<DEGREE,K> *) p->ptrs[sindex];
	
	            // tag operations take precedence
	            if (s->tag) {
	                if (p->getABDegree() + s->getABDegree() <= DEGREE+1) {
	                    if (!tagJoinParent_fast(info, tid, gp, p, gp->getChildIndex(key), s, sindex)) _xabort(ABORT_UPDATE_FAILED);
	                    return true;
	                } else {
	                    if (!tagSplit_fast(info, tid, gp, p, gp->getChildIndex(key), s, sindex)) _xabort(ABORT_UPDATE_FAILED);
	                    return true;
	                }
	            } else {
	                // either join l and s, or redistribute keys between them
	                if (l->getABDegree() + s->getABDegree() < 2*MIN_DEGREE) {
	                    if (!joinSibling_fast(info, tid, gp, p, gp->getChildIndex(key), l, lindex, s, sindex)) _xabort(ABORT_UPDATE_FAILED);
	                    return true;
	                } else {
	                    if (!redistributeSibling_fast(info, tid, gp, p, gp->getChildIndex(key), l, lindex, s, sindex)) _xabort(ABORT_UPDATE_FAILED);
	                    return true;
	                }
	            }
	        }
	        // impossible to get here
	        if (_xtest()) _xend();
	        cout<<"IMPOSSIBLE"<<endl;
	        exit(-1);
	    } else {
	aborthere:
	        if (info) info->lastAbort = status;
	//        IF_ALWAYS_RETRY_WHEN_BIT_SET if (status & _XABORT_RETRY) { this->counters->pathFail[info->path]->inc(tid); this->counters->htmRetryAbortRetried[info->path]->inc(tid); goto TXN1; }
	        return false;
	    }
	}

	bool rebalance_middle(wrapper_info<DEGREE,K> * const info, const int tid, const K& key, bool * const shouldRebalance)
	{
//	    // try to push into the next stack PAGE before starting the txn if we might encounter a page boundary
//	    char stackptr = 0;
//	    int rem = pagesize - (((long long) &stackptr) & pagesize);
//	    const int estNeeded = 4000; // estimate on the space needed for stack frames of all callees beyond this point
//	    if (rem < estNeeded) {
//	        volatile char x[rem+8];
//	        x[rem+7] = 0; // force page to load
//	    }
	    
	TXN1: int attempts = MAX_FAST_HTM_RETRIES;
	    int status = _xbegin();
	    if (status == _XBEGIN_STARTED) {
	        if (numFallback > 0) { _xabort(ABORT_PROCESS_ON_FALLBACK); }
	
	        abtree_Node<DEGREE,K> * gp = root;
	        abtree_Node<DEGREE,K> * p = root;
	        abtree_Node<DEGREE,K> * l = (abtree_Node<DEGREE,K> *) root->ptrs[0];
	
	        // Unrolled special case for root:
	        // Note: l is NOT tagged, but it might have only 1 child pointer, which would be a problem
	        if (l->isLeaf()) {
	            _xend();
	            *shouldRebalance = false; // no violations to fix
	            return true; // nothing can be wrong with the root, if it is a leaf
	        }
	        bool scxRecordReady = false;
	        if (l->getABDegree() == 1) { // root is internal and has only one child
	            scxRecordReady = rootJoinParent_fallback<true>(info, tid, p, l, p->getChildIndex(key));
	            goto doscx;
	        }
	
	        // root is internal, and there is nothing wrong with it, so move on
	        gp = p;
	        p = l;
	        l = l->getChild(key);
	
	        // check each subsequent node for tag violations and degree violations
	        while (!(l->isLeaf() || l->tag || (l->getABDegree() < MIN_DEGREE && !isSentinel(l)))) {
	            gp = p;
	            p = l;
	            l = l->getChild(key);
	        }
	
	        if (!(l->tag || (l->getABDegree() < MIN_DEGREE && !isSentinel(l)))) {
	            _xend();
	            *shouldRebalance = false; // no violations to fix
	            return true;
	        }
	
	        // tag operations take precedence
	        if (l->tag) {
	            if (p->getABDegree() + l->getABDegree() <= DEGREE+1) {
	                scxRecordReady = tagJoinParent_fallback<true>(info, tid, gp, p, gp->getChildIndex(key), l, p->getChildIndex(key));
	            } else {
	                scxRecordReady = tagSplit_fallback<true>(info, tid, gp, p, gp->getChildIndex(key), l, p->getChildIndex(key));
	            }
	        } else { // assert (l->getABDegree() < MIN_DEGREE)
	            // get sibling of l
	            abtree_Node<DEGREE,K> * s;
	            int lindex = p->getChildIndex(key);
	            //if (p->ptrs[lindex] != l) return false;
	            int sindex = lindex ? lindex-1 : lindex+1;
	            s = (abtree_Node<DEGREE,K> *) p->ptrs[sindex];
	
	            // tag operations take precedence
	            if (s->tag) {
	                if (p->getABDegree() + s->getABDegree() <= DEGREE+1) {
	                    scxRecordReady = tagJoinParent_fallback<true>(info, tid, gp, p, gp->getChildIndex(key), s, sindex);
	                } else {
	                    scxRecordReady = tagSplit_fallback<true>(info, tid, gp, p, gp->getChildIndex(key), s, sindex);
	                }
	            } else {
	                // either join l and s, or redistribute keys between them
	                if (l->getABDegree() + s->getABDegree() < 2*MIN_DEGREE) {
	                    scxRecordReady = joinSibling_fallback<true>(info, tid, gp, p, gp->getChildIndex(key), l, lindex, s, sindex);
	                } else {
	                    scxRecordReady = redistributeSibling_fallback<true>(info, tid, gp, p, gp->getChildIndex(key), l, lindex, s, sindex);
	                }
	            }
	        }
	    doscx:
	        // perform rebalancing step
	        if (scxRecordReady) {
	            bool retval = scx_txn(tid, info);
	            _xend();
	//            reclaimMemoryAfterSCX(tid, info, true);
				return retval;
	        } else {
	            _xend();
	            return false; // continue fixing violations
	        }
	    } else {
	aborthere:
	//        IF_ALWAYS_RETRY_WHEN_BIT_SET if (status & _XABORT_RETRY) { this->counters->pathFail[info->path]->inc(tid); this->counters->htmRetryAbortRetried[info->path]->inc(tid); goto TXN1; }
	        if (info) info->lastAbort = status;
	        return false;
	    }
	}

	bool rebalance_fallback(wrapper_info<DEGREE,K> * const info, const int tid, const K& key, bool * const shouldRebalance)
	{
	    abtree_Node<DEGREE,K> * gp = root;
	    abtree_Node<DEGREE,K> * p = root;
	    abtree_Node<DEGREE,K> * l = (abtree_Node<DEGREE,K> *) root->ptrs[0];
	    
	    // Unrolled special case for root:
	    // Note: l is NOT tagged, but it might have only 1 child pointer, which would be a problem
	    if (l->isLeaf()) {
	        *shouldRebalance = false; // no violations to fix
	        return true; // nothing can be wrong with the root, if it is a leaf
	    }
	    bool scxRecordReady = false;
	    if (l->getABDegree() == 1) { // root is internal and has only one child
	        scxRecordReady = rootJoinParent_fallback<false>(info, tid, p, l, p->getChildIndex(key));
	        goto doscx;
	    }
	    
	    // root is internal, and there is nothing wrong with it, so move on
	    gp = p;
	    p = l;
	    l = l->getChild(key);
	    
	    // check each subsequent node for tag violations and degree violations
	    while (!(l->isLeaf() || l->tag || (l->getABDegree() < MIN_DEGREE && !isSentinel(l)))) {
	        gp = p;
	        p = l;
	        l = l->getChild(key);
	    }
	    
	    if (!(l->tag || (l->getABDegree() < MIN_DEGREE && !isSentinel(l)))) {
	        *shouldRebalance = false; // no violations to fix
	        return true;
	    }
	    
	    // tag operations take precedence
	    if (l->tag) {
	        if (p->getABDegree() + l->getABDegree() <= DEGREE+1) {
	            scxRecordReady = tagJoinParent_fallback<false>(info, tid, gp, p, gp->getChildIndex(key), l, p->getChildIndex(key));
	        } else {
	            scxRecordReady = tagSplit_fallback<false>(info, tid, gp, p, gp->getChildIndex(key), l, p->getChildIndex(key));
	        }
	    } else { // assert (l->getABDegree() < MIN_DEGREE)
	        // get sibling of l
	        abtree_Node<DEGREE,K> * s;
	        int lindex = p->getChildIndex(key);
	        //if (p->ptrs[lindex] != l) return false;
	        int sindex = lindex ? lindex-1 : lindex+1;
	        s = (abtree_Node<DEGREE,K> *) p->ptrs[sindex];
	
	        // tag operations take precedence
	        if (s->tag) {
	            if (p->getABDegree() + s->getABDegree() <= DEGREE+1) {
	                scxRecordReady = tagJoinParent_fallback<false>(info, tid, gp, p, gp->getChildIndex(key), s, sindex);
	            } else {
	                scxRecordReady = tagSplit_fallback<false>(info, tid, gp, p, gp->getChildIndex(key), s, sindex);
	            }
	        } else {
	            // either join l and s, or redistribute keys between them
	            if (l->getABDegree() + s->getABDegree() < 2*MIN_DEGREE) {
	                scxRecordReady = joinSibling_fallback<false>(info, tid, gp, p, gp->getChildIndex(key), l, lindex, s, sindex);
	            } else {
	                scxRecordReady = redistributeSibling_fallback<false>(info, tid, gp, p, gp->getChildIndex(key), l, lindex, s, sindex);
	            }
	        }
	    }
	doscx:
	    // perform rebalancing step
	    if (scxRecordReady) {
//	        const int init_state = abtree_SCXRecord<DEGREE,K>::STATE_INPROGRESS;
//	        assert(info->allFrozen == false);
//	        assert(info->state == init_state);
//	        assert(info->numberOfNodesAllocated >= 1);
//	        assert(info->lastAbort == 0);
	        
	        bool retval = scx(tid, info);
//	        reclaimMemoryAfterSCX(tid, info, false);
			return retval;
	    }
	    return false; // continue fixing violations
	}


	bool rootJoinParent_fast(wrapper_info<DEGREE,K> * const info, const int tid, abtree_Node<DEGREE,K> * const p, abtree_Node<DEGREE,K> * const l, const int lindex)
	{
	    abtree_Node<DEGREE,K> * c = (abtree_Node<DEGREE,K> *) l->ptrs[0];
	    abtree_Node<DEGREE,K> * newNode = new abtree_Node<DEGREE,K>();
	    newNode->marked = false;
	    newNode->scxRecord = dummy;
	
	    for (int i=0;i<c->getKeyCount();++i) {
	        newNode->keys[i] = c->keys[i];
	    }
	    for (int i=0;i<c->getABDegree();++i) {
	        newNode->ptrs[i] = c->ptrs[i];
	    }
	
	    newNode->tag = false; // since p is root(holder), newNode is the new actual root, so its tag is false
	    newNode->size = c->size;
	    newNode->leaf = c->leaf;
	
	    p->ptrs[lindex] = newNode;
	    
	    _xend();
	//    REPLACE_ALLOCATED_NODE(tid, 0);
	//    recordmgr->retire(tid, c);
	//    recordmgr->retire(tid, l);
	    return true;
	}

	bool tagJoinParent_fast(wrapper_info<DEGREE,K> * const info, const int tid, abtree_Node<DEGREE,K> * const gp, abtree_Node<DEGREE,K> * const p, const int pindex, abtree_Node<DEGREE,K> * const l, const int lindex)
	{
	    // create new nodes for update
	    abtree_Node<DEGREE,K> * newp = new abtree_Node<DEGREE,K>();
	    newp->marked = false;
	    newp->scxRecord = dummy;
	
	    // elements of p left of l
	    int k1=0, k2=0;
	    for (int i=0;i<lindex;++i) {
	        newp->keys[k1++] = p->keys[i];
	    }
	    for (int i=0;i<lindex;++i) {
	        newp->ptrs[k2++] = p->ptrs[i];
	    }
	
	    // contents of l
	    for (int i=0;i<l->getKeyCount();++i) {
	        newp->keys[k1++] = l->keys[i];
	    }
	    for (int i=0;i<l->getABDegree();++i) {
	        newp->ptrs[k2++] = l->ptrs[i];
	    }
	
	    // remaining elements of p
	    for (int i=lindex;i<p->getKeyCount();++i) {
	        newp->keys[k1++] = p->keys[i];
	    }
	    // skip child pointer for lindex
	    for (int i=lindex+1;i<p->getABDegree();++i) {
	        newp->ptrs[k2++] = p->ptrs[i];
	    }
	    
	    newp->tag = false;
	    newp->size = p->size + l->size - 1;
	    newp->leaf = false;
	
	    gp->ptrs[pindex] = newp;
	
	    _xend();
	//    REPLACE_ALLOCATED_NODE(tid, 0);
	//    recordmgr->retire(tid, l);
	    return true;
	}

	bool tagSplit_fast(wrapper_info<DEGREE,K> * const info, const int tid, abtree_Node<DEGREE,K> * const gp, abtree_Node<DEGREE,K> * const p, const int pindex, abtree_Node<DEGREE,K> * const l, const int lindex)
	{
	    // create new nodes for update
	    const int sz = p->getABDegree() + l->getABDegree() - 1;
	    const int leftsz = sz/2;
	    const int rightsz = sz - leftsz;
	    
	    K keys[2*DEGREE+1];
	    void * ptrs[2*DEGREE+1];
	    int k1=0, k2=0;
	
	    // elements of p left than l
	    for (int i=0;i<lindex;++i) {
	        keys[k1++] = p->keys[i];
	    }
	    for (int i=0;i<lindex;++i) {
	        ptrs[k2++] = p->ptrs[i];
	    }
	
	    // contents of l
	    for (int i=0;i<l->getKeyCount();++i) {
	        keys[k1++] = l->keys[i];
	    }
	    for (int i=0;i<l->getABDegree();++i) {
	        ptrs[k2++] = l->ptrs[i];
	    }
	
	    // remaining elements of p
	    for (int i=lindex;i<p->getKeyCount();++i) {
	        keys[k1++] = p->keys[i];
	    }
	    // skip child pointer for lindex
	    for (int i=lindex+1;i<p->getABDegree();++i) {
	        ptrs[k2++] = p->ptrs[i];
	    }
	    
	    abtree_Node<DEGREE,K> * newp = new abtree_Node<DEGREE,K>();
	    newp->scxRecord = dummy;
	    newp->marked = false;
	
	    abtree_Node<DEGREE,K> * newleft = new abtree_Node<DEGREE,K>();
	    newleft->scxRecord = dummy;
	    newleft->marked = false;
	
	    abtree_Node<DEGREE,K> * newright = new abtree_Node<DEGREE,K>();
	    newright->scxRecord = dummy;
	    newright->marked = false;
	
	    k1=0;
	    k2=0;
	    
	    for (int i=0;i<leftsz-1;++i) {
	        newleft->keys[i] = keys[k1++];
	    }
	    for (int i=0;i<leftsz;++i) {
	        newleft->ptrs[i] = ptrs[k2++];
	    }
	    newleft->tag = false;
	    newleft->size = leftsz;
	    newleft->leaf = false;
	    
	    newp->keys[0] = keys[k1++];
	    newp->ptrs[0] = newleft;
	    newp->ptrs[1] = newright;
	    newp->tag = (gp != root);
	    newp->size = 2;
	    newp->leaf = false;
	    
	    for (int i=0;i<rightsz-1;++i) {
	        newright->keys[i] = keys[k1++];
	    }
	    for (int i=0;i<rightsz;++i) {
	        newright->ptrs[i] = ptrs[k2++];
	    }
	    newright->tag = false;
	    newright->size = rightsz;
	    newright->leaf = false;
	    
	    gp->ptrs[pindex] = newp;
	    
	    _xend();
	//    recordmgr->retire(tid, l);
	//    recordmgr->retire(tid, p);
	//    REPLACE_ALLOCATED_NODE(tid, 0);
	//    REPLACE_ALLOCATED_NODE(tid, 1);
	//    REPLACE_ALLOCATED_NODE(tid, 2);
	    return true;
	}

	bool joinSibling_fast(wrapper_info<DEGREE,K> * const info, const int tid, abtree_Node<DEGREE,K> * const gp, abtree_Node<DEGREE,K> * const p, const int pindex, abtree_Node<DEGREE,K> * const l, const int lindex, abtree_Node<DEGREE,K> * const s, const int sindex)
	{
	    // create new nodes for update
	    abtree_Node<DEGREE,K> * newp = new abtree_Node<DEGREE,K>();
	    newp->scxRecord = dummy;
	    newp->marked = false;
	
	    abtree_Node<DEGREE,K> * newl = new abtree_Node<DEGREE,K>();
	    newl->scxRecord = dummy;
	    newl->marked = false;
	
	    abtree_Node<DEGREE,K> * left;
	    abtree_Node<DEGREE,K> * right;
	    int leftindex;
	    int rightindex;
	    if (lindex < sindex) {
	        left = l;
	        leftindex = lindex;
	        right = s;
	        rightindex = sindex;
	    } else {
	        left = s;
	        leftindex = sindex;
	        right = l;
	        rightindex = lindex;
	    }
	    
	    int k1=0, k2=0;
	    for (int i=0;i<left->getKeyCount();++i) {
	        newl->keys[k1++] = left->keys[i];
	    }
	    for (int i=0;i<left->getABDegree();++i) {
	        newl->ptrs[k2++] = left->ptrs[i];
	    }
	    if (!left->isLeaf())
			newl->keys[k1++] = p->keys[leftindex];
	    for (int i=0;i<right->getKeyCount();++i) {
	        newl->keys[k1++] = right->keys[i];
	    }
	    for (int i=0;i<right->getABDegree();++i) {
	        newl->ptrs[k2++] = right->ptrs[i];
	    }
	    
	    // create newp from p by:
	    // 1. skipping the key for leftindex and child pointer for sindex
	    // 2. replacing l with newl
	    for (int i=0;i<leftindex;++i) {
	        newp->keys[i] = p->keys[i];
	    }
	    for (int i=0;i<sindex;++i) {
	        newp->ptrs[i] = p->ptrs[i];
	    }
	    for (int i=leftindex+1;i<p->getKeyCount();++i) {
	        newp->keys[i-1] = p->keys[i];
	    }
	    for (int i=sindex+1;i<p->getABDegree();++i) {
	        newp->ptrs[i-1] = p->ptrs[i];
	    }
	    // replace l with newl
	    newp->ptrs[lindex - (lindex > sindex)] = newl;
	    
	    newp->tag = false;
	    newp->size = p->size - 1;
	    newp->leaf = false;
	    newl->tag = false;
	    newl->size = l->size + s->size;
	    newl->leaf = l->leaf;
	    
	    gp->ptrs[pindex] = newp;    
	    
	    _xend();
	//    REPLACE_ALLOCATED_NODE(tid, 0);
	//    REPLACE_ALLOCATED_NODE(tid, 1);
	//    recordmgr->retire(tid, p);
	//    recordmgr->retire(tid, l);
	//    recordmgr->retire(tid, s);
	    return true;
	}

	bool redistributeSibling_fast(wrapper_info<DEGREE,K> * const info, const int tid, abtree_Node<DEGREE,K> * const gp, abtree_Node<DEGREE,K> * const p, const int pindex, abtree_Node<DEGREE,K> * const l, const int lindex, abtree_Node<DEGREE,K> * const s, const int sindex)
	{
	    // create new nodes for update
	    abtree_Node<DEGREE,K> * newp = new abtree_Node<DEGREE,K>();
	    newp->scxRecord = dummy;
	    newp->marked = false;
	
	    abtree_Node<DEGREE,K> * newl = new abtree_Node<DEGREE,K>();
	    newl->scxRecord = dummy;
	    newl->marked = false;
	
	    abtree_Node<DEGREE,K> * news = new abtree_Node<DEGREE,K>();
	    news->scxRecord = dummy;
	    news->marked = false;
	
	    // create newl and news by evenly sharing the keys + pointers of l and s
	    int sz = l->getABDegree() + s->getABDegree();
	    int leftsz = sz/2;
	    int rightsz = sz-leftsz;
	    kvpair<K,V> tosort[2*DEGREE+1];
	    
	    abtree_Node<DEGREE,K> * left;
	    abtree_Node<DEGREE,K> * right;
	    abtree_Node<DEGREE,K> * newleft;
	    abtree_Node<DEGREE,K> * newright;
	    int leftindex;
	    int rightindex;
	    if (lindex < sindex) {
	        left = l;
	        newleft = newl;
	        leftindex = lindex;
	        right = s;
	        newright = news;
	        rightindex = sindex;
	    } else {
	        left = s;
	        newleft = news;
	        leftindex = sindex;
	        right = l;
	        newright = newl;
	        rightindex = lindex;
	    }
	    assert(rightindex == 1+leftindex);
	    
	    // combine the contents of l and s (and one key from p)
	    int k1=0, k2=0;
	    for (int i=0;i<left->getKeyCount();++i) {
	        tosort[k1++].key = left->keys[i];
	    }
	    for (int i=0;i<left->getABDegree();++i) {
	        tosort[k2++].val = (V)left->ptrs[i];
	    }
	    if (!left->isLeaf())
			tosort[k1++].key = p->keys[leftindex];
	    for (int i=0;i<right->getKeyCount();++i) {
	        tosort[k1++].key = right->keys[i];
	    }
	    for (int i=0;i<right->getABDegree();++i) {
	        tosort[k2++].val = (V)right->ptrs[i];
	    }
	    //assert(k1 == sz+left->isLeaf()); // only holds in general if something like opacity is satisfied
	    assert(!gp->tag);
	    assert(!p->tag);
	    assert(!left->tag);
	    assert(!right->tag);
	    assert(k1 <= sz+1);
	    assert(k2 == sz);
	    assert(!left->isLeaf() || k1 == k2);
	    
	    // sort if this is a leaf
	    if (left->isLeaf()) qsort(tosort, k1, sizeof(kvpair<K,V>), kv_compare<K,V>);
	    
	    // distribute contents between newleft and newright
	    k1=0;
	    k2=0;
	    for (int i=0;i<leftsz - !left->isLeaf();++i) {
	        newleft->keys[i] = tosort[k1++].key;
	    }
	    for (int i=0;i<leftsz;++i) {
	        newleft->ptrs[i] = tosort[k2++].val;
	    }
	    // reserve one key for the parent (to go between newleft and newright))
	    K keyp = tosort[k1].key;
	    if (!left->isLeaf()) ++k1;
	    for (int i=0;i<rightsz - !left->isLeaf();++i) {
	        newright->keys[i] = tosort[k1++].key;
	    }
	    for (int i=0;i<rightsz;++i) {
	        newright->ptrs[i] = tosort[k2++].val;
	    }
	    
	    // create newp from p by replacing left with newleft and right with newright,
	    // and replacing one key (between these two pointers)
	    for (int i=0;i<p->getKeyCount();++i) {
	        newp->keys[i] = p->keys[i];
	    }
	    for (int i=0;i<p->getABDegree();++i) {
	        newp->ptrs[i] = p->ptrs[i];
	    }
	    newp->keys[leftindex] = keyp;
	    newp->ptrs[leftindex] = newleft;
	    newp->ptrs[rightindex] = newright;
	    newp->tag = false;
	    newp->size = p->size;
	    newp->leaf = false;
	    
	    newleft->tag = false;
	    newleft->size = leftsz;
	    newleft->leaf = left->leaf;
	    newright->tag = false;
	    newright->size = rightsz;
	    newright->leaf = right->leaf;
	
	    gp->ptrs[pindex] = newp;
	    
	    _xend();
	//    REPLACE_ALLOCATED_NODE(tid, 0);
	//    REPLACE_ALLOCATED_NODE(tid, 1);
	//    REPLACE_ALLOCATED_NODE(tid, 2);
	//    recordmgr->retire(tid, l);
	//    recordmgr->retire(tid, s);
	//    recordmgr->retire(tid, p);
	    return true;
	}

	template<bool in_txn>
	bool rootJoinParent_fallback(wrapper_info<DEGREE,K> * const info, const int tid, abtree_Node<DEGREE,K> * const p, abtree_Node<DEGREE,K> * const l, const int lindex)
	{
	    // perform LLX on p and l, and l's only child c
	    void * ptrsp[DEGREE];
	    if ((info->scxRecordsSeen[0] = (abtree_SCXRecord<DEGREE,K> *) (in_txn ? llx_txn(tid, p, ptrsp) : llx(tid, p, ptrsp))) == NULL) return false;
	    if (p->ptrs[lindex] != l) return false;
	    info->nodes[0] = p;
	
	    void * ptrsl[DEGREE];
	    if ((info->scxRecordsSeen[1] = (abtree_SCXRecord<DEGREE,K> *) (in_txn ? llx_txn(tid, l, ptrsl) : llx(tid, l, ptrsl))) == NULL) return false;
	    info->nodes[1] = l;
	    
	    void * ptrsc[DEGREE];
	    abtree_Node<DEGREE,K> * c = (abtree_Node<DEGREE,K> *) l->ptrs[0];
	    if ((info->scxRecordsSeen[2] = (abtree_SCXRecord<DEGREE,K> *) (in_txn ? llx_txn(tid, c, ptrsc) : llx(tid, c, ptrsc))) == NULL) return false;
	    info->nodes[2] = c;
	    
	    // prepare SCX record for update
	    abtree_Node<DEGREE,K> * newNode = new abtree_Node<DEGREE,K>();
	    newNode->marked = false;
	    newNode->scxRecord = dummy;
	
	    for (int i=0;i<c->getKeyCount();++i) {
	        newNode->keys[i] = c->keys[i];
	    }
	    for (int i=0;i<c->getABDegree();++i) {
	        newNode->ptrs[i] = c->ptrs[i];
	    }
	
	    assert(!p->tag);
	    assert(!l->tag);
	    assert(p == root);
	    newNode->tag = false; // since p is root(holder), newNode is the new actual root, so its tag is false
	    newNode->size = c->size;
	    newNode->leaf = c->leaf;
	
	    info->numberOfNodesAllocated = 1;
	    info->numberOfNodesToFreeze = 3;
	    info->numberOfNodes = 3;
	    info->field = &p->ptrs[lindex];
	    info->newNode = newNode;
	    return true;
	}

	template<bool in_txn>
	bool tagJoinParent_fallback(wrapper_info<DEGREE,K> * const info, const int tid, abtree_Node<DEGREE,K> * const gp, abtree_Node<DEGREE,K> * const p, const int pindex, abtree_Node<DEGREE,K> * const l, const int lindex)
	{
	    // perform LLX on gp, p and l
	    void * ptrsgp[DEGREE];
	    if ((info->scxRecordsSeen[0] = (abtree_SCXRecord<DEGREE,K> *) (in_txn ? llx_txn(tid, gp, ptrsgp) : llx(tid, gp, ptrsgp))) == NULL) return false;
	    if (gp->ptrs[pindex] != p) return false;
	    info->nodes[0] = gp;
	
	    void * ptrsp[DEGREE];
	    if ((info->scxRecordsSeen[1] = (abtree_SCXRecord<DEGREE,K> *) (in_txn ? llx_txn(tid, p, ptrsp) : llx(tid, p, ptrsp))) == NULL) return false;
	    if (p->ptrs[lindex] != l) return false;
	    info->nodes[1] = p;
	
	    void * ptrsl[DEGREE];
	    if ((info->scxRecordsSeen[2] = (abtree_SCXRecord<DEGREE,K> *) (in_txn ? llx_txn(tid, l, ptrsl) : llx(tid, l, ptrsl))) == NULL) return false;
	    info->nodes[2] = l;
	    
	    // create new nodes for update
	    abtree_Node<DEGREE,K> * newNode = new abtree_Node<DEGREE,K>();
	    newNode->marked = false;
	    newNode->scxRecord = dummy;
	
	    // elements of p left of l
	    int k1=0, k2=0;
	    for (int i=0;i<lindex;++i) {
	        newNode->keys[k1++] = p->keys[i];
	    }
	    for (int i=0;i<lindex;++i) {
	        newNode->ptrs[k2++] = p->ptrs[i];
	    }
	
	    // contents of l
	    for (int i=0;i<l->getKeyCount();++i) {
	        newNode->keys[k1++] = l->keys[i];
	    }
	    for (int i=0;i<l->getABDegree();++i) {
	        newNode->ptrs[k2++] = l->ptrs[i];
	    }
	
	    // remaining elements of p
	    for (int i=lindex;i<p->getKeyCount();++i) {
	        newNode->keys[k1++] = p->keys[i];
	    }
	    // skip child pointer for lindex
	    for (int i=lindex+1;i<p->getABDegree();++i) {
	        newNode->ptrs[k2++] = p->ptrs[i];
	    }
	    
	    newNode->tag = false;
	    newNode->size = p->size + l->size - 1;
	    newNode->leaf = false;
	    assert(!gp->tag);
	    assert(!p->tag);
	    assert(l->tag);
	    assert(k2 == newNode->size);
	    assert(k1 == newNode->size - !newNode->isLeaf());
	
	    // prepare SCX record for update
	    info->numberOfNodesAllocated = 1;
	    info->numberOfNodesToFreeze = 3;
	    info->numberOfNodes = 3;
	    info->field = &gp->ptrs[pindex];
	    info->newNode = newNode;
	    return true;
	}

	template<bool in_txn>
	bool tagSplit_fallback(wrapper_info<DEGREE,K> * const info, const int tid, abtree_Node<DEGREE,K> * const gp, abtree_Node<DEGREE,K> * const p, const int pindex, abtree_Node<DEGREE,K> * const l, const int lindex)
	{
	    // perform LLX on gp, p and l
	    void * ptrsgp[DEGREE];
	    if ((info->scxRecordsSeen[0] = (abtree_SCXRecord<DEGREE,K> *) (in_txn ? llx_txn(tid, gp, ptrsgp) : llx(tid, gp, ptrsgp))) == NULL) return false;
	    if (gp->ptrs[pindex] != p) return false;
	    info->nodes[0] = gp;
	
	    void * ptrsp[DEGREE];
	    if ((info->scxRecordsSeen[1] = (abtree_SCXRecord<DEGREE,K> *) (in_txn ? llx_txn(tid, p, ptrsp) : llx(tid, p, ptrsp))) == NULL) return false;
	    if (p->ptrs[lindex] != l) return false;
	    info->nodes[1] = p;
	
	    void * ptrsl[DEGREE];
	    if ((info->scxRecordsSeen[2] = (abtree_SCXRecord<DEGREE,K> *) (in_txn ? llx_txn(tid, l, ptrsl) : llx(tid, l, ptrsl))) == NULL) return false;
	    info->nodes[2] = l;
	    
	    // create new nodes for update
	    const int sz = p->getABDegree() + l->getABDegree() - 1;
	    const int leftsz = sz/2;
	    const int rightsz = sz - leftsz;
	    
	    K keys[sz-1];
	    void * ptrs[sz];
	    int k1=0, k2=0;
	
	    // elements of p left than l
	    for (int i=0;i<lindex;++i) {
	        keys[k1++] = p->keys[i];
	    }
	    for (int i=0;i<lindex;++i) {
	        ptrs[k2++] = p->ptrs[i];
	    }
	
	    // contents of l
	    for (int i=0;i<l->getKeyCount();++i) {
	        keys[k1++] = l->keys[i];
	    }
	    for (int i=0;i<l->getABDegree();++i) {
	        ptrs[k2++] = l->ptrs[i];
	    }
	
	    // remaining elements of p
	    for (int i=lindex;i<p->getKeyCount();++i) {
	        keys[k1++] = p->keys[i];
	    }
	    // skip child pointer for lindex
	    for (int i=lindex+1;i<p->getABDegree();++i) {
	        ptrs[k2++] = p->ptrs[i];
	    }
	    assert(!gp->tag);
	    assert(!p->tag);
	    assert(l->tag);
	    assert(k1 <= sz-1);
	    assert(k2 <= sz);
	    
	    abtree_Node<DEGREE,K> * newp = new abtree_Node<DEGREE,K>();
	    newp->scxRecord = dummy;
	    newp->marked = false;
	
	    abtree_Node<DEGREE,K> * newleft = new abtree_Node<DEGREE,K>();
	    newleft->scxRecord = dummy;
	    newleft->marked = false;
	
	    abtree_Node<DEGREE,K> * newright = new abtree_Node<DEGREE,K>();
	    newright->scxRecord = dummy;
	    newright->marked = false;
	
	    k1=0;
	    k2=0;
	    
	    for (int i=0;i<leftsz-1;++i) {
	        newleft->keys[i] = keys[k1++];
	    }
	    for (int i=0;i<leftsz;++i) {
	        newleft->ptrs[i] = ptrs[k2++];
	    }
	    newleft->tag = false;
	    newleft->size = leftsz;
	    newleft->leaf = false;
	    
	    newp->keys[0] = keys[k1++];
	    newp->ptrs[0] = newleft;
	    newp->ptrs[1] = newright;
	    newp->tag = (gp != root);
	    newp->size = 2;
	    newp->leaf = false;
	    
	    for (int i=0;i<rightsz-1;++i) {
	        newright->keys[i] = keys[k1++];
	    }
	    for (int i=0;i<rightsz;++i) {
	        newright->ptrs[i] = ptrs[k2++];
	    }
	    newright->tag = false;
	    newright->size = rightsz;
	    newright->leaf = false;
	    
	    // prepare SCX record for update
	    info->numberOfNodesAllocated = 3;
	    info->numberOfNodesToFreeze = 3;
	    info->numberOfNodes = 3;
	    info->field = &gp->ptrs[pindex];
	    info->newNode = newp;
	    return true;
	}

	template<bool in_txn>
	bool joinSibling_fallback(wrapper_info<DEGREE,K> * const info, const int tid, abtree_Node<DEGREE,K> * const gp, abtree_Node<DEGREE,K> * const p, const int pindex, abtree_Node<DEGREE,K> * const l, const int lindex, abtree_Node<DEGREE,K> * const s, const int sindex)
	{
	    // perform LLX on gp, p and l
	    void * ptrsgp[DEGREE];
	    if ((info->scxRecordsSeen[0] = (abtree_SCXRecord<DEGREE,K> *) (in_txn ? llx_txn(tid, gp, ptrsgp) : llx(tid, gp, ptrsgp))) == NULL) return false;
	    if (gp->ptrs[pindex] != p) return false;
	    info->nodes[0] = gp;
	    
	    void * ptrsp[DEGREE];
	    if ((info->scxRecordsSeen[1] = (abtree_SCXRecord<DEGREE,K> *) (in_txn ? llx_txn(tid, p, ptrsp) : llx(tid, p, ptrsp))) == NULL) return false;
	    if (p->ptrs[lindex] != l) return false;
	    if (p->ptrs[sindex] != s) return false;
	    info->nodes[1] = p;
	
	    int freezeorderl = (sindex > lindex) ? 2 : 3;
	    int freezeorders = (sindex > lindex) ? 3 : 2;
	    
	    void * ptrsl[DEGREE];
	    if ((info->scxRecordsSeen[freezeorderl] = (abtree_SCXRecord<DEGREE,K> *) (in_txn ? llx_txn(tid, l, ptrsl) : llx(tid, l, ptrsl))) == NULL) return false;
	    info->nodes[freezeorderl] = l;
	
	    void * ptrss[DEGREE];
	    if ((info->scxRecordsSeen[freezeorders] = (abtree_SCXRecord<DEGREE,K> *) (in_txn ? llx_txn(tid, s, ptrss) : llx(tid, s, ptrss))) == NULL) return false;
	    info->nodes[freezeorders] = s;
	    
	    // create new nodes for update
	    abtree_Node<DEGREE,K> * newp = new abtree_Node<DEGREE,K>();
	    newp->scxRecord = dummy;
	    newp->marked = false;
	
	    abtree_Node<DEGREE,K> * newl = new abtree_Node<DEGREE,K>();
	    newl->scxRecord = dummy;
	    newl->marked = false;
	
	    // create newl by joining s to l
	
	    abtree_Node<DEGREE,K> * left;
	    abtree_Node<DEGREE,K> * right;
	    int leftindex;
	    int rightindex;
	    if (lindex < sindex) {
	        left = l;
	        leftindex = lindex;
	        right = s;
	        rightindex = sindex;
	    } else {
	        left = s;
	        leftindex = sindex;
	        right = l;
	        rightindex = lindex;
	    }
	    
	    int k1=0, k2=0;
	    for (int i=0;i<left->getKeyCount();++i) {
	        newl->keys[k1++] = left->keys[i];
	    }
	    for (int i=0;i<left->getABDegree();++i) {
	        newl->ptrs[k2++] = left->ptrs[i];
	    }
	    if (!left->isLeaf())
			newl->keys[k1++] = p->keys[leftindex];
	    for (int i=0;i<right->getKeyCount();++i) {
	        newl->keys[k1++] = right->keys[i];
	    }
	    for (int i=0;i<right->getABDegree();++i) {
	        newl->ptrs[k2++] = right->ptrs[i];
	    }
	    
	    // create newp from p by:
	    // 1. skipping the key for leftindex and child pointer for sindex
	    // 2. replacing l with newl
	    for (int i=0;i<leftindex;++i) {
	        newp->keys[i] = p->keys[i];
	    }
	    for (int i=0;i<sindex;++i) {
	        newp->ptrs[i] = p->ptrs[i];
	    }
	    for (int i=leftindex+1;i<p->getKeyCount();++i) {
	        newp->keys[i-1] = p->keys[i];
	    }
	    for (int i=sindex+1;i<p->getABDegree();++i) {
	        newp->ptrs[i-1] = p->ptrs[i];
	    }
	    // replace l with newl
	    newp->ptrs[lindex - (lindex > sindex)] = newl;
	    
	    newp->tag = false;
	    newp->size = p->size - 1;
	    newp->leaf = false;
	    newl->tag = false;
	    newl->size = l->size + s->size;
	    newl->leaf = l->leaf;
	    
	    assert(!gp->tag);
	    assert(!p->tag);
	    assert(!left->tag);
	    assert(!right->tag);
	    assert(k2 == newl->size);
	    if (k1 != newl->size - !newl->isLeaf()) {
			cout << "SOMETHING WENT WRONG!!!\n";
	        exit(-1);
	    }
	    assert(k1 == newl->size - !newl->isLeaf());
	
	    // prepare SCX record for update
	    info->numberOfNodesAllocated = 2;
	    info->numberOfNodesToFreeze = 4;
	    info->numberOfNodes = 4;
	    info->field = &gp->ptrs[pindex];
	    info->newNode = newp;
	    
	    return true;
	}

	template<bool in_txn>
	bool redistributeSibling_fallback(wrapper_info<DEGREE,K> * const info, const int tid, abtree_Node<DEGREE,K> * const gp, abtree_Node<DEGREE,K> * const p, const int pindex, abtree_Node<DEGREE,K> * const l, const int lindex, abtree_Node<DEGREE,K> * const s, const int sindex)
	{
	    // perform LLX on gp, p, l and s
	    void * ptrsgp[DEGREE];
	    if ((info->scxRecordsSeen[0] = (abtree_SCXRecord<DEGREE,K> *) (in_txn ? llx_txn(tid, gp, ptrsgp) : llx(tid, gp, ptrsgp))) == NULL) return false;
	    if (gp->ptrs[pindex] != p) return false;
	    info->nodes[0] = gp;
	    
	    void * ptrsp[DEGREE];
	    if ((info->scxRecordsSeen[1] = (abtree_SCXRecord<DEGREE,K> *) (in_txn ? llx_txn(tid, p, ptrsp) : llx(tid, p, ptrsp))) == NULL) return false;
	    if (p->ptrs[lindex] != l) return false;
	    if (p->ptrs[sindex] != s) return false;
	    info->nodes[1] = p;
	
	    int freezeorderl = (sindex > lindex) ? 2 : 3;
	    int freezeorders = (sindex > lindex) ? 3 : 2;
	    
	    void * ptrsl[DEGREE];
	    if ((info->scxRecordsSeen[freezeorderl] = (abtree_SCXRecord<DEGREE,K> *) (in_txn ? llx_txn(tid, l, ptrsl) : llx(tid, l, ptrsl))) == NULL) return false;
	    info->nodes[freezeorderl] = l;
	
	    void * ptrss[DEGREE];
	    if ((info->scxRecordsSeen[freezeorders] = (abtree_SCXRecord<DEGREE,K> *) (in_txn ? llx_txn(tid, s, ptrss) : llx(tid, s, ptrss))) == NULL) return false;
	    info->nodes[freezeorders] = s;
	
	    // create new nodes for update
	    abtree_Node<DEGREE,K> * newp = new abtree_Node<DEGREE,K>();
	    newp->scxRecord = dummy;
	    newp->marked = false;
	
	    abtree_Node<DEGREE,K> * newl = new abtree_Node<DEGREE,K>();
	    newl->scxRecord = dummy;
	    newl->marked = false;
	
	    abtree_Node<DEGREE,K> * news = new abtree_Node<DEGREE,K>();
	    news->scxRecord = dummy;
	    news->marked = false;
	
	    // create newl and news by evenly sharing the keys + pointers of l and s
	    int sz = l->getABDegree() + s->getABDegree();
	    int leftsz = sz/2;
	    int rightsz = sz-leftsz;
	    kvpair<K,V> tosort[sz+1];
	    
	    abtree_Node<DEGREE,K> * left;
	    abtree_Node<DEGREE,K> * right;
	    abtree_Node<DEGREE,K> * newleft;
	    abtree_Node<DEGREE,K> * newright;
	    int leftindex;
	    int rightindex;
	    if (lindex < sindex) {
	        left = l;
	        newleft = newl;
	        leftindex = lindex;
	        right = s;
	        newright = news;
	        rightindex = sindex;
	    } else {
	        left = s;
	        newleft = news;
	        leftindex = sindex;
	        right = l;
	        newright = newl;
	        rightindex = lindex;
	    }
	    assert(rightindex == 1+leftindex);
	    
	    // combine the contents of l and s (and one key from p)
	    int k1=0, k2=0;
	    for (int i=0;i<left->getKeyCount();++i) {
	        tosort[k1++].key = left->keys[i];
	    }
	    for (int i=0;i<left->getABDegree();++i) {
	        tosort[k2++].val = (V)left->ptrs[i];
	    }
	    if (!left->isLeaf())
			tosort[k1++].key = p->keys[leftindex];
	    for (int i=0;i<right->getKeyCount();++i) {
	        tosort[k1++].key = right->keys[i];
	    }
	    for (int i=0;i<right->getABDegree();++i) {
	        tosort[k2++].val = (V)right->ptrs[i];
	    }
	    //assert(k1 == sz+left->isLeaf()); // only holds in general if something like opacity is satisfied
	    assert(!gp->tag);
	    assert(!p->tag);
	    assert(!left->tag);
	    assert(!right->tag);
	    assert(k1 <= sz+1);
	    assert(k2 == sz);
	    assert(!left->isLeaf() || k1 == k2);
	    
	    // sort if this is a leaf
	    if (left->isLeaf()) qsort(tosort, k1, sizeof(kvpair<K,V>), kv_compare<K,V>);
	    
	    // distribute contents between newleft and newright
	    k1=0;
	    k2=0;
	    for (int i=0;i<leftsz - !left->isLeaf();++i) {
	        newleft->keys[i] = tosort[k1++].key;
	    }
	    for (int i=0;i<leftsz;++i) {
	        newleft->ptrs[i] = tosort[k2++].val;
	    }
	    // reserve one key for the parent (to go between newleft and newright))
	//    K keyp = tosort[k1].key;
		K keyp;
		keyp = tosort[k1].key;
	    if (!left->isLeaf()) ++k1;
	    for (int i=0;i<rightsz - !left->isLeaf();++i) {
	        newright->keys[i] = tosort[k1++].key;
	    }
	    for (int i=0;i<rightsz;++i) {
	        newright->ptrs[i] = tosort[k2++].val;
	    }
	    
	    // create newp from p by replacing left with newleft and right with newright,
	    // and replacing one key (between these two pointers)
	    for (int i=0;i<p->getKeyCount();++i) {
	        newp->keys[i] = p->keys[i];
	    }
	    for (int i=0;i<p->getABDegree();++i) {
	        newp->ptrs[i] = p->ptrs[i];
	    }
	    newp->keys[leftindex] = keyp;
	    newp->ptrs[leftindex] = newleft;
	    newp->ptrs[rightindex] = newright;
	    newp->tag = false;
	    newp->size = p->size;
	    newp->leaf = false;
	    
	    newleft->tag = false;
	    newleft->size = leftsz;
	    newleft->leaf = left->leaf;
	    newright->tag = false;
	    newright->size = rightsz;
	    newright->leaf = right->leaf;
	
	    // prepare SCX record for update
	    info->numberOfNodesAllocated = 3;
	    info->numberOfNodesToFreeze = 4;
	    info->numberOfNodes = 4;
	    info->field = &gp->ptrs[pindex];
	    info->newNode = newp;
	    
	    return true;
	}

private:
	bool scx_txn(const int tid, wrapper_info<DEGREE,K> * info)
	{
	    abtree_SCXRecord<DEGREE,K> * scx = (abtree_SCXRecord<DEGREE,K> *) NEXT_VERSION_NUMBER(tid);
	    for (int i=0;i<info->numberOfNodesToFreeze;++i) {
	        if (info->scxRecordsSeen[i] == LLX_RETURN_IS_LEAF) continue; // do not freeze leaves
	        info->nodes[i]->scxRecord = scx;
	    }
	    for (int i=1;i<info->numberOfNodes;++i) {
	        if (info->scxRecordsSeen[i] == LLX_RETURN_IS_LEAF) continue; // do not mark leaves
	        info->nodes[i]->marked = true;
	    }
	    *(info->field) = (void*) info->newNode;
	    info->state = abtree_SCXRecord<DEGREE,K>::STATE_COMMITTED;
	    return true;
	}

	void *llx_txn(const int tid, abtree_Node<DEGREE,K> * node, void **retPointers)
	{
	    abtree_SCXRecord<DEGREE,K> *scx1 = node->scxRecord;
	    int state = (IS_VERSION_NUMBER(scx1) ? abtree_SCXRecord<DEGREE,K>::STATE_COMMITTED : scx1->state);
	    bool marked = node->marked;
	    if (marked) {
	        return NULL;
	    } else if (state) {
	        if (node->isLeaf()) {   // if node is a leaf, we return a special value
	            return (void *) LLX_RETURN_IS_LEAF;
	        } else {                // otherwise, we read all mutable fields
	            for (int i=0;i<node->size;++i) {
	                retPointers[i] = node->ptrs[i];
	            }
	            return scx1;
	        }
	    }
	    return NULL; // fail
	}

	bool scx(const int tid, wrapper_info<DEGREE,K> * info)
	{
	    const int init_state = abtree_SCXRecord<DEGREE,K>::STATE_INPROGRESS;
	    abtree_SCXRecord<DEGREE,K> * rec = createSCXRecord(tid, info->field, info->newNode, info->nodes, info->scxRecordsSeen, info->numberOfNodes, info->numberOfNodesToFreeze);
	    bool result = help(tid, rec, false) & abtree_SCXRecord<DEGREE,K>::STATE_COMMITTED;
	    info->state = rec->state;
	    return result;
	}

	int help(const int tid, abtree_SCXRecord<DEGREE,K> *scx, bool helpingOther)
	{
	    const int nFreeze                                               = scx->numberOfNodesToFreeze;
	    const int nNodes                                                = scx->numberOfNodes;
	    abtree_Node<DEGREE,K> * volatile * const nodes                  = scx->nodes;
	    abtree_SCXRecord<DEGREE,K> * volatile * const scxRecordsSeen    = scx->scxRecordsSeen;
	    abtree_Node<DEGREE,K> volatile * const newNode                  = scx->newNode;
	    //SOFTWARE_BARRIER; // prevent compiler from reordering read(state) before read(nodes), read(scxRecordsSeen), read(newNode). an x86/64 cpu will not reorder these reads.
	    int __state = scx->state;
	    if (__state != abtree_SCXRecord<DEGREE,K>::STATE_INPROGRESS) { // TODO: optimize by taking this out, somehow?
	        return __state;
	    }
	
	    int flags = 1; // bit i is 1 if nodes[i] is not a leaf, and 0 otherwise.
	    for (int i=helpingOther; i<nFreeze; ++i) {
	        if (scxRecordsSeen[i] == LLX_RETURN_IS_LEAF) {
	            continue; // do not freeze leaves
	        }
	        
	        bool successfulCAS = __sync_bool_compare_and_swap(&nodes[i]->scxRecord, scxRecordsSeen[i], scx);
	        abtree_SCXRecord<DEGREE,K> * exp = nodes[i]->scxRecord;
	        if (!successfulCAS && exp != scx) { // if work was not done
	
	            if (scx->allFrozen) {
	                return abtree_SCXRecord<DEGREE,K>::STATE_COMMITTED; // success
	            } else {
	                if (i == 0) {
	                    scx->state = ABORT_STATE_INIT(0, 0); // scx is aborted (but no one else will ever know)
	                    return ABORT_STATE_INIT(0, 0);
	                } else {
	                    int expectedState = abtree_SCXRecord<DEGREE,K>::STATE_INPROGRESS;
	                    int newState = ABORT_STATE_INIT(i, flags);
	                    bool success = __sync_bool_compare_and_swap(&scx->state, expectedState, newState);     // MEMBAR ON X86/64
	                    expectedState = scx->state;
	                    const int state_aborted = abtree_SCXRecord<DEGREE,K>::STATE_ABORTED; // alias needed since the :: causes problems with the assert() macro, below
	                    if (success) {
	                        return newState;
	                    } else {
	                        return expectedState; // this has been overwritten by compare_exchange_strong with the value that caused the CAS to fail.
	                    }
	                }
	            }
	        } else {
	            flags |= (1<<i); // nodes[i] was frozen for scx
	            const int state_inprogress = abtree_SCXRecord<DEGREE,K>::STATE_INPROGRESS;
	        }
	    }
	    scx->allFrozen = true;
	    SOFTWARE_BARRIER;
	    for (int i=1; i<nFreeze; ++i) {
	        if (scxRecordsSeen[i] == LLX_RETURN_IS_LEAF) continue; // do not mark leaves
	        nodes[i]->marked = true; // finalize all but first node
	    }
	
	    abtree_Node<DEGREE,K> * expected = nodes[1];
	    __sync_bool_compare_and_swap(scx->field, expected, newNode);
	    scx->state = abtree_SCXRecord<DEGREE,K>::STATE_COMMITTED;
	    
	    return abtree_SCXRecord<DEGREE,K>::STATE_COMMITTED; // success
	}

	void *llx(const int tid, abtree_Node<DEGREE,K> *node, void **retPointers)
	{
	    abtree_SCXRecord<DEGREE,K> *scx1 = node->scxRecord;
	    int state = (IS_VERSION_NUMBER(scx1) ? abtree_SCXRecord<DEGREE,K>::STATE_COMMITTED : scx1->state);
	    SOFTWARE_BARRIER;       // prevent compiler from moving the read of marked before the read of state (no hw barrier needed on x86/64, since there is no read-read reordering)
	    int marked = node->marked;
	    SOFTWARE_BARRIER;       // prevent compiler from moving the reads scx2=node->scxRecord or scx3=node->scxRecord before the read of marked. (no h/w barrier needed on x86/64 since there is no read-read reordering)
	    if ((state & abtree_SCXRecord<DEGREE,K>::STATE_COMMITTED && !marked) || state & abtree_SCXRecord<DEGREE,K>::STATE_ABORTED) {
	        SOFTWARE_BARRIER;       // prevent compiler from moving the reads scx2=node->scxRecord or scx3=node->scxRecord before the read of marked. (no h/w barrier needed on x86/64 since there is no read-read reordering)
	        if (node->isLeaf()) {
	            // if node is a leaf, we return a special value
	            return (void *) LLX_RETURN_IS_LEAF;
	        } else {
	            // otherwise, we read all mutable fields
	            for (int i=0;i<node->size;++i) {
	                retPointers[i] = node->ptrs[i];
	            }
	        }
	        SOFTWARE_BARRIER; // prevent compiler from moving the read of node->scxRecord before the reads of node's mutable fields
	        abtree_SCXRecord<DEGREE,K> *scx2 = node->scxRecord;
	        if (scx1 == scx2) {
	            // on x86/64, we do not need any memory barrier here to prevent mutable fields of node from being moved before our read of scx1, because the hardware does not perform read-read reordering. on another platform, we would need to ensure no read from after this point is reordered before this point (technically, before the read that becomes scx1)...
	            return scx1;    // success
	        } else {
//	            if (recordmgr->shouldHelp()) {
				if (1) {
	                if (!IS_VERSION_NUMBER(scx2)) {
	                    help(tid, scx2, true);
	                }
	            }
	        }
	    } else if (state == abtree_SCXRecord<DEGREE,K>::STATE_INPROGRESS) {
//	        if (recordmgr->shouldHelp()) {
			if (1) {
	            if (!IS_VERSION_NUMBER(scx1)) {
	                help(tid, scx1, true);
	            }
	        }
	    } else {
	        // state committed and marked
//	        if (recordmgr->shouldHelp()) {
			if (1) {
	            abtree_SCXRecord<DEGREE,K> *scx3 = node->scxRecord;
	            if (!IS_VERSION_NUMBER(scx3)) {
	                help(tid, scx3, true);
	            }
	        } else {
	        }
	    }
	    return NULL;            // fail
	}

private:
        /*******************************************************************
         * Utility functions for integration with the test harness
         *******************************************************************/
        int sequentialSize(abtree_Node<DEGREE,K> *node) {
            if (node->isLeaf()) return node->getKeyCount();
            int retval = 0;
            for (int i=0;i<node->getABDegree();++i) {
                abtree_Node<DEGREE,K> *child = (abtree_Node<DEGREE,K> *)node->ptrs[i];
                retval += sequentialSize(child);
            }
            return retval;
        }
        int sequentialSize() {
            return sequentialSize((abtree_Node<DEGREE,K> *)root->ptrs[0]);
        }

        int getNumberOfLeaves(abtree_Node<DEGREE,K> *node) {
            if (node == NULL) return 0;
            if (node->isLeaf()) return 1;
            int result = 0;
            for (int i=0;i<node->getABDegree();++i)
                result += getNumberOfLeaves((abtree_Node<DEGREE,K> *)node->ptrs[i]);
            return result;
        }
        const int getNumberOfLeaves() {
            return getNumberOfLeaves((abtree_Node<DEGREE,K> *)root->ptrs[0]);
        }
        int getNumberOfInternals(abtree_Node<DEGREE,K> *node) {
            if (node == NULL) return 0;
            if (node->isLeaf()) return 0;
            int result = 1;
            for (int i=0;i<node->getABDegree();++i)
                result += getNumberOfInternals((abtree_Node<DEGREE,K> *)node->ptrs[i]);
            return result;
        }
        const int getNumberOfInternals() {
            return getNumberOfInternals((abtree_Node<DEGREE,K> *)root->ptrs[0]);
        }
        const int getNumberOfNodes() {
            return getNumberOfLeaves() + getNumberOfInternals();
        }

        int getSumOfKeyDepths(abtree_Node<DEGREE,K> *node, int depth) {
            if (node == NULL) return 0;
            if (node->isLeaf()) return depth * node->getKeyCount();
            int result = 0;
            for (int i=0;i<node->getABDegree();i++)
                result += getSumOfKeyDepths((abtree_Node<DEGREE,K> *)node->ptrs[i], 1+depth);
            return result;
        }
        const int getSumOfKeyDepths() {
            return getSumOfKeyDepths((abtree_Node<DEGREE,K> *)root->ptrs[0], 0);
        }
        const double getAverageKeyDepth() {
            long sz = sequentialSize();
            return (sz == 0) ? 0 : getSumOfKeyDepths() / sz;
        }

        int getHeight(abtree_Node<DEGREE,K> *node, int depth) {
            if (node == NULL) return 0;
            if (node->isLeaf()) return 0;
            int result = 0;
            for (int i=0;i<node->getABDegree();i++) {
                int retval = getHeight((abtree_Node<DEGREE,K> *)node->ptrs[i], 1+depth);
                if (retval > result) result = retval;
            }
            return result+1;
        }
        const int getHeight() {
            return getHeight((abtree_Node<DEGREE,K> *)root->ptrs[0], 0);
        }

        int getKeyCount(abtree_Node<DEGREE,K> *node) {
            if (node == NULL) return 0;
            if (node->isLeaf()) return node->getKeyCount();
            int sum = 0;
            for (int i=0;i<node->getABDegree();++i)
                sum += getKeyCount((abtree_Node<DEGREE,K> *)root->ptrs[i]);
            return sum;
        }
        int getTotalDegree(abtree_Node<DEGREE,K> *node) {
            if (node == NULL) return 0;
            int sum = node->getKeyCount();
            if (node->isLeaf()) return sum;
            for (int i=0;i<node->getABDegree();++i) {
                sum += getTotalDegree((abtree_Node<DEGREE,K> *)node->ptrs[i]);
            }
            return 1+sum; // one more children than keys
        }
        int getNodeCount(abtree_Node<DEGREE,K> *node) {
            if (node == NULL) return 0;
            if (node->isLeaf()) return 1;
            int sum = 1;
            for (int i=0;i<node->getABDegree();++i) {
                sum += getNodeCount((abtree_Node<DEGREE,K> *)node->ptrs[i]);
            }
            return sum;
        }
        double getAverageDegree() {
            return getTotalDegree(root) / (double) getNodeCount(root);
        }
        double getSpacePerKey() {
            return getNodeCount(root)*2*DEGREE / (double) getKeyCount(root);
        }

        long long getSumOfKeys(abtree_Node<DEGREE,K> *node) {
            long long sum = 0;
            if (node->isLeaf()) {
                for (int i=0;i<node->getKeyCount();++i)
                    sum += (long long) node->keys[i];
            } else {
                for (int i=0;i<node->getABDegree();++i)
                    sum += getSumOfKeys((abtree_Node<DEGREE,K> *)node->ptrs[i]);
            }
            return sum;
        }
        long long getSumOfKeys() {
            return getSumOfKeys(root);
        }

        void debugPrint() {
			std::cout << "\nValidation:\n";
			std::cout << "=======================\n";
            std::cout << "averageDegree= "  << getAverageDegree()     << "\n";
            std::cout << "averageDepth= "   << getAverageKeyDepth()   << "\n";
            std::cout << "height= "         << getHeight()            << "\n";
            std::cout << "internalNodes= "  << getNumberOfInternals() << "\n";
            std::cout << "leafNodes= "      << getNumberOfLeaves()    << "\n";
			std::cout << "sequentialSize= " << sequentialSize()       << "\n";
        }
        /*******************************************************************/

};

#define ABTREE_BROWN_3PATH_TEMPL template<typename K, typename V, int DEGREE, int MIN_DEGREE>
#define ABTREE_BROWN_3PATH_FUNCT abtree_brown_3path<K,V,DEGREE,MIN_DEGREE>

ABTREE_BROWN_3PATH_TEMPL
bool ABTREE_BROWN_3PATH_FUNCT::contains(const int tid, const K& key)
{
	const V ret = lookup_helper(tid, key);
	return (ret != this->NO_VALUE);
}

ABTREE_BROWN_3PATH_TEMPL
const std::pair<V,bool> ABTREE_BROWN_3PATH_FUNCT::find(const int tid, const K& key)
{
	const V ret = lookup_helper(tid, key);
	return std::pair<V,bool>(ret, ret != this->NO_VALUE);
}

ABTREE_BROWN_3PATH_TEMPL
int ABTREE_BROWN_3PATH_FUNCT::rangeQuery(const int tid, const K& lo, const K& hi,
                      std::vector<std::pair<K,V>> kv_pairs)
{
	return 0;
}

ABTREE_BROWN_3PATH_TEMPL
const V ABTREE_BROWN_3PATH_FUNCT::insert(const int tid, const K& key, const V& val)
{
	return insert_helper(tid, key, val, false);
}

ABTREE_BROWN_3PATH_TEMPL
const V ABTREE_BROWN_3PATH_FUNCT::insertIfAbsent(const int tid, const K& key, const V& val)
{
	return insert_helper(tid, key, val, true);
}

ABTREE_BROWN_3PATH_TEMPL
const std::pair<V,bool> ABTREE_BROWN_3PATH_FUNCT::remove(const int tid, const K& key)
{
	const V ret = delete_helper(tid, key);
	return std::pair<V,bool>(ret, ret != this->NO_VALUE);
}

ABTREE_BROWN_3PATH_TEMPL
bool ABTREE_BROWN_3PATH_FUNCT::validate()
{
	debugPrint();
	return false;
}
