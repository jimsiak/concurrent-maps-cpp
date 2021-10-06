/**
 * An external interpolation search tree.
 * Paper:
 *    Non-blocking interpolation search trees with doubly-logarithmic running time, Brown et. al, PPoPP 2020
 **/

/**
 * Notes about the key and value type that can be used:
 *   - Key '0' can not be inserted.
 *   - INF_KEY needs to be provided.
 *   - NO_VALUE needs to be provided.
 **/

#pragma once

#include <cmath>

#include "../../../lib/Keygen.h" //> For RandomFNV1A
#include "../../map_if.h"
#include "Log.h"
#include "dcss.h"

//#define IST_DISABLE_MULTICOUNTER_AT_ROOT
//#define NO_REBUILDING
//#define IST_DISABLE_COLLABORATIVE_MARK_AND_COUNT
#define MAX_ACCEPTABLE_LEAF_SIZE (48)

//> Note: the following are hacky macros to essentially replace polymorphic
//>       types since polymorphic types are unnecessarily expensive. A child
//>       pointer in a node can actually represent several different things:a
//>       pointer to another node, a pointer to a key-value pair, a pointer to a
//>       rebuild object, or a value. To figure out which is the case, use the
//>       macros IS_[NODE|KVPAIR|REBUILDOP|VAL]. To cast neutral casword_t types
//>       to pointers to these objects,
//>       use CASWORD_TO_[NODE|KVPAIR|REBUILDOP|VAL].
//>       To cast object pointers to casword_t, use the macros
//>       [NODE|KVPAIR|REBUILDOP|VAL]_TO_CASWORD. There is additionally a
//>       special reserved/distinguished "EMPTY" value, which can be identified
//>       by using IS_EMPTY_VAL.
//>       To store an empty value, use EMPTY_VAL_TO_CASWORD.

// for fields Node::ptr(...)
#define TYPE_MASK                   (0x6ll)
#define DCSS_BITS                   (1)
#define TYPE_BITS                   (2)
#define TOTAL_BITS                  (DCSS_BITS+TYPE_BITS)
#define TOTAL_MASK                  (0x7ll)

#define NODE_MASK                   (0x0ll) /* no need to use this... 0 mask is implicit */
#define IS_NODE(x)                  (((x)&TYPE_MASK)==NODE_MASK)
#define CASWORD_TO_NODE(x)          ((Node *) (x))
#define NODE_TO_CASWORD(x)          ((casword_t) (x))

#define KVPAIR_MASK                 (0x2ll) /* 0x1 is used by DCSS */
#define IS_KVPAIR(x)                (((x)&TYPE_MASK)==KVPAIR_MASK)
#define CASWORD_TO_KVPAIR(x)        ((KVPair *) ((x)&~TYPE_MASK))
#define KVPAIR_TO_CASWORD(x)        ((casword_t) (((casword_t) (x))|KVPAIR_MASK))

#define REBUILDOP_MASK              (0x4ll)
#define IS_REBUILDOP(x)             (((x)&TYPE_MASK)==REBUILDOP_MASK)
#define CASWORD_TO_REBUILDOP(x)     ((RebuildOperation *) ((x)&~TYPE_MASK))
#define REBUILDOP_TO_CASWORD(x)     ((casword_t) (((casword_t) (x))|REBUILDOP_MASK))

#define VAL_MASK                    (0x6ll)
#define IS_VAL(x)                   (((x)&TYPE_MASK)==VAL_MASK)
#define CASWORD_TO_VAL(x)           ((V) ((x)>>TOTAL_BITS))
#define VAL_TO_CASWORD(x)           ((casword_t) ((((casword_t) (x))<<TOTAL_BITS)|VAL_MASK))

#define EMPTY_VAL_TO_CASWORD        (((casword_t) ~TOTAL_MASK) | VAL_MASK)
#define IS_EMPTY_VAL(x)             (((casword_t) (x)) == EMPTY_VAL_TO_CASWORD)

// for field Node::dirty
// note: dirty finished should imply dirty started!
#define DIRTY_STARTED_MASK          (0x1ll)
#define DIRTY_FINISHED_MASK         (0x2ll)
#define DIRTY_MARKED_FOR_FREE_MASK  (0x4ll) /* used for memory reclamation */
#define IS_DIRTY_STARTED(x)         ((x)&DIRTY_STARTED_MASK)
#define IS_DIRTY_FINISHED(x)        ((x)&DIRTY_FINISHED_MASK)
#define IS_DIRTY_MARKED_FOR_FREE(x) ((x)&DIRTY_MARKED_FOR_FREE_MASK)
#define SUM_TO_DIRTY_FINISHED(x)    (((x)<<3)|DIRTY_FINISHED_MASK|DIRTY_STARTED_MASK)
#define DIRTY_FINISHED_TO_SUM(x)    ((x)>>3)

// constants for rebuilding
//> any subtree will be rebuilt after a number of updates equal to this fraction
//> of its size are performed; example: after 250k updates in a subtree that
//> contained 1M keys at the time it was last rebuilt, it will be rebuilt again
#define REBUILD_FRACTION            (0.25) 


template <typename K, typename V>
class ist_brown : public Map<K,V> {
public:
	ist_brown(const K _NO_KEY, const V _NO_VALUE, const int numProcesses)
	  : Map<K,V>(_NO_KEY, _NO_VALUE),
	    prov(new dcssProvider<void *>(numProcesses))
	{
		const int tid = 0;
		initThread(tid);

		root = createNode(tid, 1);
		root->minKey = INF_KEY;
		root->maxKey = INF_KEY;
		*root->ptrAddr(0) = EMPTY_VAL_TO_CASWORD;
	}

	void initThread(const int tid) {
		threadRNGs[tid].set_seed(rand());
		assert(threadRNGs[tid].next());
		prov->initThread(tid);
	};
	void deinitThread(const int tid) {
		prov->deinitThread(tid);
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
	char *name() { return "IST Brown"; }

	void print() { print_helper(); };
//	unsigned long long size() { return size_rec(root) - 2; };

private:

	//> FIXME this should not be here, but somewhere global
	const K INF_KEY = 9999999;
	const int NUM_PROCESSES = 88;

	//> FIXME this should not be here, but somewhere global
	class MultiCounter {
	private:
		volatile size_t * const counters;
		const int numCounters;
	public:
		MultiCounter(const int numThreads, const int sizeMultiple)
		        : counters(new size_t[std::max(2, sizeMultiple*numThreads)+1])
		        , numCounters(std::max(2, sizeMultiple*numThreads)) {
			for (int i=0;i<numCounters+1;++i)
				counters[i] = 0;
		}
		~MultiCounter() {
			delete[] counters;
		}
		inline size_t inc(const int tid, RandomFNV1A * rng, const size_t amt = 1) {
			const int i = rng->next(numCounters);
			int j;
			do { j = rng->next(numCounters); } while (i == j);
			size_t vi = counters[1+i];
			size_t vj = counters[1+j];
			return __sync_fetch_and_add((vi < vj ? &counters[1+i] : &counters[1+j]), amt) + amt;
		}
		inline size_t readFast(const int tid, RandomFNV1A * rng) {
			const int i = rng->next(numCounters);
			return numCounters * counters[1+i];
		}
		size_t readAccurate() {
			size_t sum = 0;
			for (int i=0;i<numCounters;++i)
				sum += counters[1+i];
			return sum;
		}
	};


	struct KVPair {
		K k;
		V v;
	};

	struct Node {
		/**
		 * NOTE: unlisted fields: `degree-1` keys of type K followed by `degree`
		 * values/pointers of type casword_t
		 * The values/pointers have tags in their 3 LSBs so that they satisfy
		 * either IS_NODE, IS_KVPAIR, IS_REBUILDOP or IS_VAL
		 **/
	
		size_t volatile degree;

		//> field not *technically* needed (used to avoid loading extra cache
		//> lines for interpolationSearch in the common case, buying for time
		//> for prefetching while interpolation arithmetic occurs)
		K minKey;

		//> field not *technically* needed (same as above)
		K maxKey;

		//> field likely not needed (but convenient and good for debug asserts)
		size_t capacity;

		//> initial size (at time of last rebuild) of the subtree rooted at this node
		size_t initSize;

		//> 2-LSBs are marked by markAndCount; also stores the number of pairs in
		//> a subtree as recorded by markAndCount (see SUM_TO_DIRTY_FINISHED and
		//> DIRTY_FINISHED_TO_SUM)
		size_t volatile dirty;

		//> facilitates recursive-collaborative markAndCount() by allowing threads
		//> to dynamically soft-partition subtrees (NOT workstealing/exclusive
		//> access - this is still a lock-free mechanism)
		size_t volatile nextMarkAndCount;

		//> could be merged with initSize above (subtract make initSize 1/4 of what
		//> it would normally be, then subtract from it instead of incrementing
		//> changeSum, and rebuild when it hits zero)
		volatile size_t changeSum;

		#ifndef IST_DISABLE_MULTICOUNTER_AT_ROOT
		//> NULL for all nodes except the root (or top few nodes), and supercedes
		//> changeSum when non-NULL.
		MultiCounter *externalChangeCounter;
		#endif

		inline K *keyAddr(const int ix) {
			K * const firstKey = ((K *) (((char *) this)+sizeof(Node)));
			return &firstKey[ix];
		}
		inline K& key(const int ix) {
			assert(ix >= 0);
			assert((volatile size_t)ix < degree - 1);
			return *keyAddr(ix);
		}
		// conceptually returns &node.ptrs[ix]
		inline casword_t volatile *ptrAddr(const int ix) {
			assert(ix >= 0);
			assert((volatile size_t)ix < degree);
			K * const firstKeyAfter = keyAddr(degree - 1);
			casword_t * const firstPtr = (casword_t *) firstKeyAfter;
			return &firstPtr[ix];
		}
	
		// conceptually returns node.ptrs[ix]
		inline casword_t volatile ptr(const int ix) {
			return *ptrAddr(ix);
		}

		inline void incrementChangeSum(const int tid, RandomFNV1A *rng) {
			#ifndef IST_DISABLE_MULTICOUNTER_AT_ROOT
			if (externalChangeCounter == NULL)
				__sync_fetch_and_add(&changeSum, 1);
			else
				externalChangeCounter->inc(tid, rng);
			#else
			__sync_fetch_and_add(&changeSum, 1);
			#endif
		}
		inline size_t readChangeSum(const int tid, RandomFNV1A *rng) {
			#ifndef IST_DISABLE_MULTICOUNTER_AT_ROOT
			if (externalChangeCounter == NULL)
				return changeSum;
			else
				return externalChangeCounter->readFast(tid, rng);
			#else
			return changeSum;
			#endif
		}

		void print() {
			std::cout << "[ ";
			for (size_t i=0; i < degree-2; i++)
				std::cout << key(i) << " | ";
			std::cout << key(degree-2) << " ]\n";
		}
	};

	enum UpdateType {
		InsertIfAbsent,
		InsertReplace,
		Erase
	};

private:
	RandomFNV1A threadRNGs[88];
	dcssProvider<void* /* unused */> * const prov;
	Node *root;

private:
	KVPair *createKVPair(const int tid, const K& key, const V& value)
	{
		KVPair *result = new KVPair(); 
		*result = { key, value };
		assert((((size_t) result) & TOTAL_MASK) == 0);
		assert(result);
		return result;
	}

	Node *createNode(const int tid, const int degree)
	{
		size_t sz = sizeof(Node) + sizeof(K) * (degree - 1) + sizeof(casword_t) * degree;
		Node *node = (Node *) ::operator new (sz);
		assert((((size_t) node) & TOTAL_MASK) == 0);
		node->degree = degree;
		node->capacity = 0;
		node->initSize = 0;
		node->changeSum = 0;
		#ifndef IST_DISABLE_MULTICOUNTER_AT_ROOT
		node->externalChangeCounter = NULL;
		assert(!node->externalChangeCounter);
		#endif
		node->dirty = 0;
		node->nextMarkAndCount = 0;
		assert(node);
		return node;
	}

	Node *createLeaf(const int tid, KVPair *pairs, int numPairs)
	{
		Node *node = createNode(tid, numPairs+1);
		node->degree = numPairs+1;
		node->initSize = numPairs;
		*node->ptrAddr(0) = EMPTY_VAL_TO_CASWORD;
		for (int i=0;i<numPairs;++i) {
			assert(i==0 || pairs[i].k > pairs[i-1].k);
			node->key(i) = pairs[i].k;
			*node->ptrAddr(i+1) = VAL_TO_CASWORD(pairs[i].v);
		}
		node->minKey = node->key(0);
		node->maxKey = node->key(node->degree-2);
		return node;
	}

	Node *createMultiCounterNode(const int tid, const int degree)
	{
		Node *node = createNode(tid, degree);
		#ifndef IST_DISABLE_MULTICOUNTER_AT_ROOT
		node->externalChangeCounter = new MultiCounter(this->NUM_PROCESSES, 1);
		assert(node->externalChangeCounter);
		#endif
		return node;
	}

	int interpolationSearch(const int tid, const K& key, Node * const node)
	{
		__builtin_prefetch(&node->minKey, 1);
		__builtin_prefetch(&node->maxKey, 1);

		//> these next 3 prefetches are shockingly effective...
		//> 20% performance boost in some large scale search-only workloads...
		//> (reducing L3 cache misses by 2-3 per search...)
		__builtin_prefetch((node->keyAddr(0)), 1);
		__builtin_prefetch((node->keyAddr(0))+(8), 1);
		__builtin_prefetch((node->keyAddr(0))+(16), 1);
	
		size_t volatile deg = node->degree;
		
		if (deg == 1) return 0;
		
		const int numKeys = deg - 1;
		const K& minKey = node->minKey;
		const K& maxKey = node->maxKey;
		
		if (key < minKey) return 0;
		if (key >= maxKey) return numKeys;

		// assert: minKey <= key < maxKey
		int ix = (numKeys * (key - minKey) / (maxKey - minKey));
	
		__builtin_prefetch((node->keyAddr(0))+(ix-8), 1);
		__builtin_prefetch((node->keyAddr(0))+(ix), 1);
		__builtin_prefetch((node->keyAddr(0))+(ix+8), 1);
		
		const K& ixKey = node->key(ix);
		if (key < ixKey) {
			// search to the left for node.key[i] <= key, then return i+1
			for (int i=ix-1;i>=0;--i)
				if (key >= node->key(i)) return i+1;
			assert(false);
		} else if (key > ixKey) {
			// recall: degree - 1 keys vs degree pointers
			for (int i=ix+1;i<numKeys;++i)
				if (key < node->key(i)) return i;
			assert(false);
		} else {
			return ix+1;
		}
	}

	//> Returns:
	//>   0 in case of DCSS success
	//>   1 if retryNode is necessary
	//>   2 if retry is necessary
	//>   3 if foundVal can be immediately returned.
	int doUpdate_helper(const int tid, const K& key, const V& val, UpdateType t,
	                    casword_t word, Node *node, int ix,
	                    bool &affectsChangeSum, V& foundVal)
	{
		KVPair *pair = NULL;
		Node *newNode = NULL;
		KVPair *newPair = NULL;
		casword_t newWord = (casword_t)NULL;
		
		assert(IS_EMPTY_VAL(word) || !IS_VAL(word) || ix > 0);
		K foundKey = INF_KEY;
		foundVal = this->NO_VALUE;
		if (IS_VAL(word)) {
			if (!IS_EMPTY_VAL(word)) {
				foundKey = node->key(ix-1);
				foundVal = CASWORD_TO_VAL(word);
			}
		} else {
			assert(IS_KVPAIR(word));
			pair = CASWORD_TO_KVPAIR(word);
			foundKey = pair->k;
			foundVal = pair->v;
		}
	
		 // value must have top 3 bits empty so we can shift it
		assert(foundVal == this->NO_VALUE ||
		       (((size_t)foundVal << 3) >> 3) == (size_t)foundVal);
	
		if (foundKey == key) {
			if (t == InsertReplace) {
				newWord = VAL_TO_CASWORD(val);
				if (foundVal != this->NO_VALUE) affectsChangeSum = false; 
			} else if (t == InsertIfAbsent) {
				if (foundVal != this->NO_VALUE) return 3;
				newWord = VAL_TO_CASWORD(val);
			} else {
				assert(t == Erase);
				if (foundVal == this->NO_VALUE) return 3;
				newWord = EMPTY_VAL_TO_CASWORD;
			}
		} else {
			if (t == InsertReplace || t == InsertIfAbsent) {
				if (foundVal == this->NO_VALUE) {
					// after the insert, this pointer will lead to only one
					// kvpair in the tree so we just create a kvpair instead of
					// a node
					newPair = createKVPair(tid, key, val);
					newWord = KVPAIR_TO_CASWORD(newPair);
				} else {
					// there would be 2 kvpairs, so we create a node
					KVPair pairs[2];
					if (key < foundKey) {
						pairs[0] = { key, val };
						pairs[1] = { foundKey, foundVal };
					} else {
						pairs[0] = { foundKey, foundVal };
						pairs[1] = { key, val };
					}
					newNode = createLeaf(tid, pairs, 2);
					newWord = NODE_TO_CASWORD(newNode);
					foundVal = this->NO_VALUE; // the key we are inserting had no current value
				}
			} else {
				assert(t == Erase);
				foundVal = this->NO_VALUE;
				return 3;
			}
		}
		assert(newWord);
		assert((newWord & (~TOTAL_MASK)));
	
		assert(ix >= 0);
		assert((volatile size_t)ix < node->degree);

		// DCSS that performs the update
		auto result = prov->dcssPtr(tid, (casword_t *)&node->dirty, 0, (casword_t *)node->ptrAddr(ix), word, newWord);
		switch (result.status) {
		case DCSS_FAILED_ADDR2:
//			if (newPair) recordmgr->deallocate(tid, newPair);
//			if (newNode) freeNode(tid, newNode, false);
			return 1;
		case DCSS_FAILED_ADDR1:
//			if (newPair) recordmgr->deallocate(tid, newPair);
//			if (newNode) freeNode(tid, newNode, false);
			return 2;
		case DCSS_SUCCESS:
//			if (pair) recordmgr->retire(tid, pair);
			return 0;
		default:
			assert(0);
		}
		assert(0);
	}

	void rebuild_if_necessary(const int tid, Node **path, int pathLength,
	                          bool affectsChangeSum)
	{
		if (!affectsChangeSum) return;

		for (int i=0;i<pathLength;++i)
			path[i]->incrementChangeSum(tid, &threadRNGs[tid]);

		// now, we must determine whether we should rebuild
		for (int i=0; i < pathLength; ++i) {
			if (path[i]->readChangeSum(tid, &threadRNGs[tid]) >= REBUILD_FRACTION * path[i]->initSize) {
				if (i == 0) {
					#ifndef NO_REBUILDING
					assert(path[0]);
					rebuild(tid, path[0], root, 0, 0);
					#endif
				} else {
					Node *parent = path[i-1];
					assert(parent->degree > 1);
					assert(path[i]->degree > 1);
					int index = interpolationSearch(tid, path[i]->key(0), parent);
					
					#ifndef NO_REBUILDING
					assert(path[i]);
					rebuild(tid, path[i], parent, index, i);
					#endif
				}
				break;
			}
		}
	}

	// note: val is unused if t == Erase
	V doUpdate(const int tid, const K& key, const V& val, UpdateType t)
	{
		// top 3 bits of values must be unused!
		assert(((uint64_t) val & 0xE000000000000000ULL) == 0);
	
		//> in practice, the depth is probably less than 10 even for many
		//> billions of keys. max is technically nthreads + O(log log n),
		//> but this requires an astronomically unlikely event.
		const int MAX_PATH_LENGTH = 20;
		Node *path[MAX_PATH_LENGTH]; // stack to save the path
		Node *node;
		int pathLength;
	
	retry:
		pathLength = 0;
		node = root;
		while (true) {
			int ix = interpolationSearch(tid, key, node);

	retryNode:
			bool affectsChangeSum = true;
			casword_t word = prov->readPtr(tid, node->ptrAddr(ix));

			if (IS_KVPAIR(word) || IS_VAL(word)) {
				V foundVal;
				int ret = doUpdate_helper(tid, key, val, t, word, node, ix,
				                          affectsChangeSum, foundVal);
				switch (ret) {
				case 0: 
					rebuild_if_necessary(tid, path, pathLength, affectsChangeSum);
					return foundVal;
				case 1:
					goto retryNode;
				case 2:
					goto retry;
				case 3:
					return foundVal;
				}
			} else if (IS_REBUILDOP(word)) {
				helpRebuild(tid, CASWORD_TO_REBUILDOP(word));
				goto retry;
			} else {
				assert(IS_NODE(word));
				node = CASWORD_TO_NODE(word);
				path[pathLength++] = node; // push on stack
				assert(pathLength <= MAX_PATH_LENGTH);
			}
		}
	}

	V lookup_helper(const int tid, const K& key) {
		casword_t ptr = prov->readPtr(tid, root->ptrAddr(0));
		assert(ptr);
		Node *parent = root;
		int ixToPtr = 0;
		while (true) {
			if (IS_KVPAIR(ptr)) {
				KVPair *kv = CASWORD_TO_KVPAIR(ptr);
				return (kv->k == key) ? kv->v : this->NO_VALUE;
			} else if (IS_REBUILDOP(ptr)) {
				auto rebuild = CASWORD_TO_REBUILDOP(ptr);
				ptr = NODE_TO_CASWORD(rebuild->rebuildRoot);
			} else if (IS_NODE(ptr)) {
				parent = CASWORD_TO_NODE(ptr);
				assert(parent);
				ixToPtr = interpolationSearch(tid, key, parent);
				ptr = prov->readPtr(tid, parent->ptrAddr(ixToPtr));
			} else {
				assert(IS_VAL(ptr));
				//> invariant: leftmost pointer cannot contain a non-empty VAL
				//> (it contains a non-NULL pointer or an empty val casword)
				assert(IS_EMPTY_VAL(ptr) || ixToPtr > 0); 
				if (IS_EMPTY_VAL(ptr)) return this->NO_VALUE;
				V v = CASWORD_TO_VAL(ptr);
				int ixToKey = ixToPtr - 1;
				return (parent->key(ixToKey) == key) ? v : this->NO_VALUE;
			}
		}
	}

	int bst_violations, total_nodes, total_keys, leaf_keys;
	int num_values, num_kvpairs, num_nullvalues;
	int shortest_path, longest_path;
	/**
	 * Validates the following:
	 * 1. Keys inside node are sorted.
	 * 2. Keys inside node are higher or equal to min and less than max.
	 **/
	void node_validate(Node *n, K min, K max)
	{
		//> 1. Keys inside node are sorted.
		for (size_t i=1; i < n->degree-1; i++)
			if (n->key(i) <= n->key(i-1))
				bst_violations++;

		//> 2. Keys inside node are higher than min and less than or equal to max.
		if ( n->key(0) < min || n->key(n->degree-2) >= max)
			bst_violations++;
	}

	void validate_rec(casword_t word, K min, K max, int level)
	{
		if (!word) return;

		if (IS_NODE(word)) {
			Node *n = CASWORD_TO_NODE(word);
			total_nodes++;
			total_keys += n->degree-1;

			node_validate(n, min, max);

			for (size_t i=0; i < n->degree; i++)
				validate_rec(*n->ptrAddr(i), i == 0 ? min : n->key(i-1),
				                            i == n->degree-1 ? max : n->key(i),
				                            level+1);
		} else if (IS_KVPAIR(word)) {
			num_kvpairs++;
		} else if (IS_VAL(word)) {
			if (word == EMPTY_VAL_TO_CASWORD) num_nullvalues++;
			else                              num_values++;
		}

		if (IS_KVPAIR(word) || IS_VAL(word)) {
			if (level < shortest_path) shortest_path = level;
			if (level > longest_path) longest_path = level;
		}
	}
	
	bool validate_helper()
	{
		bool check_bst = 0;
		bst_violations = 0;
		total_nodes = total_keys = 0;
		num_values = num_kvpairs = num_nullvalues = 0;
		shortest_path = 999999;
		longest_path = -1;
	
		validate_rec(*root->ptrAddr(0), 0, INF_KEY, 0);
	
		check_bst = (bst_violations == 0);
	
		printf("Validation:\n");
		printf("=======================\n");
		printf("  BST Violation: %s\n",
		       check_bst ? "No [OK]" : "Yes [ERROR]");
		printf("  Tree size: %8d\n", total_nodes);
		printf("  Number of keys: %8d total / %8d in leaves\n", total_keys, leaf_keys);
		printf("  Number of key-value pairs: %8d\n", num_kvpairs);
		printf("  Number of values (Not NULL / NULL): %8d / %8d\n", num_values, num_nullvalues);
		printf("  Total values stored in tree: %8d\n", num_values + num_kvpairs);
		printf("  Shortest / Longest path: %4d / %4d\n", shortest_path, longest_path);
		printf("\n");
	
		return check_bst;
	}

	void print_rec(casword_t ptr, int level)
	{
		assert(ptr);
		printf("[LVL %4d]: ", level);
		fflush(stdout);

		if (IS_KVPAIR(ptr)) {
			KVPair *kvpair = CASWORD_TO_KVPAIR(ptr);
			std::cout << "  KVPair: (" << kvpair->k << "," << kvpair->v << ")\n";
		} else if (IS_NODE(ptr)) {
			Node *n = CASWORD_TO_NODE(ptr);
			n->print();
			for (size_t i=0; i < n->degree; i++)
				print_rec((casword_t)n->ptr(i), level+1);
		} else if (IS_REBUILDOP(ptr)) {
			RebuildOperation *op = CASWORD_TO_REBUILDOP(ptr);
			std::cout << "  RebuildOP: (" << op->rebuildRoot << ", "
			          << op->parent << ", " << op->index << ")\n";
			print_rec((casword_t)op->rebuildRoot, level+1);
		} else {
			V val = CASWORD_TO_VAL(ptr);
			std::cout << "  Value: (" << val << ")\n";
		}
	}
	void print_helper()
	{
		if (IS_EMPTY_VAL(*root->ptrAddr(0))) std::cout << "[empty]\n";
		else print_rec(NODE_TO_CASWORD(root->ptr(0)), 0);
	}

private:
	/*************************************************************************/
	/*                         Rebuild operations                            */
	/*************************************************************************/
	class IdealBuilder {
	private:
		size_t initNumKeys;
		ist_brown *ist;
		size_t depth;
		KVPair *pairs;
		size_t pairsAdded;
		casword_t tree;
		
		Node *build(const int tid, KVPair *pset, int psetSize, const size_t currDepth,
		            casword_t volatile *constructingSubtree, bool parallelizeWithOMP = false) {
			// bail early if tree was already constructed by someone else
			if (*constructingSubtree != NODE_TO_CASWORD(NULL))
				return NODE_TO_CASWORD(NULL); 

			//> All key-value pairs can fit in a leaf
			if (psetSize <= MAX_ACCEPTABLE_LEAF_SIZE)
				return ist->createLeaf(tid, pset, psetSize);

			//> remainder is the number of children with childSize+1 pair subsets
			//> (the other (numChildren - remainder) children have childSize pair subsets)
			double numChildrenD = std::sqrt((double) psetSize);
			size_t numChildren = (size_t) std::ceil(numChildrenD);
			size_t childSize = psetSize / (size_t) numChildren;
			size_t remainder = psetSize % numChildren;

			Node *node = NULL;
			#ifndef IST_DISABLE_MULTICOUNTER_AT_ROOT
			if (currDepth <= 1) node = ist->createMultiCounterNode(tid, numChildren);
			else                node = ist->createNode(tid, numChildren);
			#else
			node = ist->createNode(tid, numChildren);
			#endif
			node->initSize = psetSize;

//			if (parallelizeWithOMP) {
//				#pragma omp parallel
//				{
//					#ifdef _OPENMP
//					auto sub_thread_id = omp_get_thread_num();
//					#else
//					auto sub_thread_id = tid; // it will just be this main thread
//					#endif
//					ist->initThread(sub_thread_id);
//					
//					#pragma omp for
//					for (size_t i=0;i<numChildren;++i) {
//						int sz = childSize + (i < remainder);
//						KVPair *childSet = pset + i*sz + (i >= remainder ? remainder : 0);
//						auto child = build(sub_thread_id, childSet, sz, 1+currDepth, constructingSubtree);
//						
//						*node->ptrAddr(i) = NODE_TO_CASWORD(child);
//						if (i > 0) node->key(i-1) = childSet[0].k;
//					}
//				}
//			} else {
				KVPair *childSet = pset;
				for (size_t i=0; i<numChildren; ++i) {
					int sz = childSize + (i < remainder);
					Node *child = build(tid, childSet, sz, 1+currDepth, constructingSubtree);
					
					*node->ptrAddr(i) = NODE_TO_CASWORD(child);
					if (i > 0) {
						assert(child == NODE_TO_CASWORD(NULL) || child->degree > 1);
						node->key(i-1) = childSet[0].k;
					}
					childSet += sz;
				}
//			}
			node->minKey = node->key(0);
			node->maxKey = node->key(node->degree-2);
			return node;
		}

	public:
		IdealBuilder(ist_brown *ist, const size_t initNumKeys, const size_t depth) {
			this->initNumKeys = initNumKeys;
			this->ist = ist;
			this->depth = depth;
			this->pairs = new KVPair[initNumKeys];
			this->pairsAdded = 0;
			this->tree = (casword_t)NULL;
		}
		~IdealBuilder() {
			delete[] pairs;
		}
		void addKV(const int tid, const K& key, const V& value) {
			pairs[pairsAdded++] = {key, value};
			if (pairsAdded > initNumKeys) std::cout << "SKAAAATA1 " << pairsAdded << " " << initNumKeys << "\n";
			assert(pairsAdded <= initNumKeys);
		}
		casword_t getCASWord(const int tid, casword_t volatile *constructingSubtree)
		{
			if (*constructingSubtree != NODE_TO_CASWORD(NULL))
				return NODE_TO_CASWORD(NULL);

			if (pairsAdded != initNumKeys) std::cout << "SKAAAATA2 " << pairsAdded << " " << initNumKeys << "\n";
			assert(pairsAdded == initNumKeys);
			if (!tree) {
				if (pairsAdded == 0)
					tree = EMPTY_VAL_TO_CASWORD;
				else if (pairsAdded == 1)
					tree = KVPAIR_TO_CASWORD(ist->createKVPair(tid, pairs[0].k, pairs[0].v));
				else
					tree = NODE_TO_CASWORD(build(tid, pairs, pairsAdded, depth, constructingSubtree));
			}

			if (*constructingSubtree != NODE_TO_CASWORD(NULL)) {
				ist->freeSubtree(tid, tree, false);
				return NODE_TO_CASWORD(NULL);
			}

			return tree;
		}
		
		K getMinKey() {
			assert(pairsAdded > 0);
			return pairs[0].k;
		}
	};

	struct RebuildOperation {
		Node *rebuildRoot;
		Node *parent;
		size_t index;
		size_t depth;
		casword_t volatile newRoot;
		bool volatile success;

		RebuildOperation(Node *_rebuildRoot, Node *_parent, size_t _index, size_t _depth)
		  : rebuildRoot(_rebuildRoot), parent(_parent), index(_index), depth(_depth),
		    newRoot(NODE_TO_CASWORD(NULL)), success(false) {};
	};

	void rebuild(const int tid, Node *rebuildRoot, Node *parent, int indexOfRebuildRoot,
	             const size_t depth)
	{
		RebuildOperation *op = new RebuildOperation(rebuildRoot, parent, indexOfRebuildRoot, depth);
		casword_t ptr = REBUILDOP_TO_CASWORD(op);
		casword_t old = NODE_TO_CASWORD(op->rebuildRoot);
		assert(op->parent == parent);
		auto result = prov->dcssPtr(tid, (casword_t *) &op->parent->dirty, 0,
		                            (casword_t *) op->parent->ptrAddr(op->index),
		                            old, ptr);
		if (result.status == DCSS_SUCCESS) {
			helpRebuild(tid, op);
		} else {
			// in this case, we have exclusive access to free op.
			// this is because we are the only ones who will try to perform a DCSS
			// to insert op into the data structure.
			assert(result.status == DCSS_FAILED_ADDR1 || result.status == DCSS_FAILED_ADDR2);
//			recordmgr->deallocate(tid, op);
		}
	}

	void helpRebuild(const int tid, RebuildOperation *op)
	{
		size_t keyCount = markAndCount(tid, NODE_TO_CASWORD(op->rebuildRoot));
		casword_t oldWord = REBUILDOP_TO_CASWORD(op);

		casword_t newWord = createIdealConcurrent(tid, op, keyCount);
//		IdealBuilder b (this, keyCount, op->depth);
//		casword_t dummy = NODE_TO_CASWORD(NULL);
//		addKVPairs(tid, NODE_TO_CASWORD(op->rebuildRoot), &b);
//		casword_t newWord = b.getCASWord(tid, &dummy);

		// someone else already *finished* helping
		// TODO: help to free old subtree?
		if (newWord == NODE_TO_CASWORD(NULL)) return; 

		auto result = prov->dcssPtr(tid, (casword_t *) &op->parent->dirty, 0,
		                            (casword_t *) op->parent->ptrAddr(op->index),
		                            oldWord, newWord).status;
		if (result == DCSS_SUCCESS) {
			assert(op->success == false);
			op->success = true;
//			recordmgr->retire(tid, op);
		} else {
			// if we fail to CAS, then either:
			// 1. someone else CAS'd exactly newWord into op->parent->ptrAddr(op->index), or
			// 2. this rebuildop is part of a subtree that is marked and rebuilt by another rebuildop,
			//    and this DCSS failed because op->parent->dirty == 1.
			//    in this case, we should try to reclaim the subtree at newWord.
			//
			if (result == DCSS_FAILED_ADDR1) {
				// [[failed because dirty (subsumed by another rebuild operation)]]
				// note: a rebuild operation should almost never be subsumed by one started higher up,
				// because it's unlikely that while we are trying to
				// rebuild one subtree another rebuild just so happens to start above
				// (since one will only start if it was ineligible to start when we began our own reconstruction,
				//  then enough operations are performed to make a higher tree eligible for rebuild,
				//  then we finish our own rebuilding and try to DCSS our new subtree in)
				// to test this: let's measure whether this happens...
				// apparently it does happen... in a 100% update workload for 15sec with 192 threads, we have: sum rebuild_is_subsumed_at_depth by_index=0 210 1887 277 5 
				//      these numbers represent how many subsumptions happened at each depth (none at depth 0 (impossible), 210 at depth 1, and so on).
				//      regardless, this is not a performance issue for now. (at most 3 of these calls took 10ms+; the rest were below that threshold.)
				//      *if* it becomes an issue then helpFreeSubtree or something like it should fix the problem.

				// try to claim the NEW subtree located at op->newWord for reclamation
//				if (op->newRoot != NODE_TO_CASWORD(NULL)
//				    && __sync_bool_compare_and_swap(&op->newRoot, newWord, EMPTY_VAL_TO_CASWORD)) {
//					freeSubtree(tid, newWord, true);
//					// note that other threads might be trying to help our rebuildop,
//					// and so might be accessing the subtree at newWord.
//					// so, we use retire rather than deallocate.
//				}
//				// otherwise, someone else reclaimed the NEW subtree
//				assert(op->newRoot == EMPTY_VAL_TO_CASWORD);
			} else {
				assert(result == DCSS_FAILED_ADDR2);
			}
		}

		// collaboratively free the old subtree, if appropriate (if it was actually replaced)
		if (op->success) {
			assert(op->rebuildRoot);
			if (op->rebuildRoot->degree < 256) {
				// this thread was the one whose DCSS operation performed the actual swap
				if (result == DCSS_SUCCESS)
					freeSubtree(tid, NODE_TO_CASWORD(op->rebuildRoot), true);
			} else {
				#ifdef IST_DISABLE_COLLABORATIVE_FREE_SUBTREE
				if (result == DCSS_SUCCESS) freeSubtree(tid, NODE_TO_CASWORD(op->rebuildRoot), true);
				#else
				helpFreeSubtree(tid, op->rebuildRoot);
				#endif
			}
		}
	}

	size_t markAndCount(const int tid, const casword_t ptr)
	{
		if (IS_KVPAIR(ptr)) return 1;
		if (IS_VAL(ptr)) return 1 - IS_EMPTY_VAL(ptr);

		//> if we are here seeing this rebuildop, then we ALREADY marked the
		//> node that points to the rebuildop, which means that rebuild op
		//> cannot possibly change that node to effect the rebuilding.
		if (IS_REBUILDOP(ptr))
			return markAndCount(tid, NODE_TO_CASWORD(CASWORD_TO_REBUILDOP(ptr)->rebuildRoot));
	    
		assert(IS_NODE(ptr));
		Node *node = CASWORD_TO_NODE(ptr);
	    
		// optimize by taking the sum from node->dirty if we run into a finished subtree.
		// markAndCount has already FINISHED in this subtree, and sum is the count
		auto result = node->dirty;
		if (IS_DIRTY_FINISHED(result))
			return DIRTY_FINISHED_TO_SUM(result); 
	
		if (!IS_DIRTY_STARTED(result))
			__sync_val_compare_and_swap(&node->dirty, 0, DIRTY_STARTED_MASK);

		// high level idea: if not at a leaf, try to divide work between any helpers at this node
		//      by using fetch&add to "soft-reserve" a subtree to work on.
		//      (each helper will get a different subtree!)
		// note that all helpers must still try to help ALL subtrees after, though,
		//      since a helper might crash after soft-reserving a subtree.
		//      the DIRTY_FINISHED indicator makes these final helping attempts more efficient.
		//
		// this entire idea of dividing work between helpers first can be disabled
		//      by defining IST_DISABLE_COLLABORATIVE_MARK_AND_COUNT
		//
		// can the clean fetch&add work division be adapted better for concurrent ideal tree construction?
		//
		// note: could i save a second traversal to build KVPair arrays by having
		//      each thread call addKVPair for each key it sees in THIS traversal?
		//      (maybe avoiding sort order issues by saving per-thread lists and merging)
	    
		#if !defined IST_DISABLE_COLLABORATIVE_MARK_AND_COUNT
		// optimize for contention by first claiming a subtree to recurse on
		// THEN after there are no more subtrees to claim, help (any that are still DIRTY_STARTED)
		if (node->degree > MAX_ACCEPTABLE_LEAF_SIZE) { // prevent this optimization from being applied at the leaves, where the number of fetch&adds will be needlessly high
			while (1) {
				auto ix = __sync_fetch_and_add(&node->nextMarkAndCount, 1);
				if (ix >= node->degree) break;
				markAndCount(tid, prov->readPtr(tid, node->ptrAddr(ix)));

				// markAndCount has already FINISHED in this subtree, and sum is the count
				auto result = node->dirty;
				if (IS_DIRTY_FINISHED(result)) return DIRTY_FINISHED_TO_SUM(result);
			}
		}
		#endif
	    
		// recurse over all subtrees
		size_t keyCount = 0;
		for (size_t i=0; i<node->degree; ++i) {
			keyCount += markAndCount(tid, prov->readPtr(tid, node->ptrAddr(i)));

			// markAndCount has already FINISHED in this subtree, and sum is the count
			auto result = node->dirty;
			if (IS_DIRTY_FINISHED(result)) return DIRTY_FINISHED_TO_SUM(result);
		}

		__sync_bool_compare_and_swap(&node->dirty, DIRTY_STARTED_MASK, SUM_TO_DIRTY_FINISHED(keyCount));
		return keyCount;
	}

	casword_t createIdealConcurrent(const int tid, RebuildOperation *op,
	                                const size_t keyCount)
	{
		// Note: the following could be encapsulated in a ConcurrentIdealBuilder class
	    
		if (keyCount == 0) return EMPTY_VAL_TO_CASWORD;

		// remainder is the number of children with childSize+1 pair subsets
		// (the other (numChildren - remainder) children have childSize pair subsets)
		double numChildrenD = std::sqrt((double) keyCount);
		size_t numChildren = (size_t) std::ceil(numChildrenD);
		size_t childSize = keyCount / (size_t) numChildren;
		size_t remainder = keyCount % numChildren;
	    
		casword_t word = NODE_TO_CASWORD(NULL);
		casword_t newRoot = op->newRoot;
		if (newRoot == EMPTY_VAL_TO_CASWORD) {
			return NODE_TO_CASWORD(NULL);
		} else if (newRoot != NODE_TO_CASWORD(NULL)) {
			word = newRoot;
		} else {
			assert(newRoot == NODE_TO_CASWORD(NULL));

			if (keyCount <= MAX_ACCEPTABLE_LEAF_SIZE) {
				IdealBuilder b (this, keyCount, op->depth);
				casword_t dummy = NODE_TO_CASWORD(NULL);
				addKVPairs(tid, NODE_TO_CASWORD(op->rebuildRoot), &b);
				word = b.getCASWord(tid, &dummy);
				assert(word != NODE_TO_CASWORD(NULL));
			} else {
				#ifndef IST_DISABLE_MULTICOUNTER_AT_ROOT
				if (op->depth <= 1)
					word = NODE_TO_CASWORD(createMultiCounterNode(tid, numChildren));
				else
				#endif
					word = NODE_TO_CASWORD(createNode(tid, numChildren));
			
				for (size_t i=0;i<CASWORD_TO_NODE(word)->degree;++i)
					*CASWORD_TO_NODE(word)->ptrAddr(i) = NODE_TO_CASWORD(NULL);
			}
	        
			// try to CAS node into the RebuildOp
			if (__sync_bool_compare_and_swap(&op->newRoot, NODE_TO_CASWORD(NULL), word)) { 
				// this should (and will) fail if op->newRoot == EMPTY_VAL_TO_CASWORD because helping is done
				assert(word != NODE_TO_CASWORD(NULL));
			} else {
				// we failed the newRoot CAS, so we lost the consensus race.
				// someone else CAS'd their newRoot in, so ours is NOT the new root.
				// reclaim ours, and help theirs instead.
				freeSubtree(tid, word, false);
				
				// try to help theirs
				word = op->newRoot;
				assert(word != NODE_TO_CASWORD(NULL));
				if (word == EMPTY_VAL_TO_CASWORD) {
					// this rebuildop was part of a subtree that was rebuilt,
					// and someone else CAS'd the newRoot from non-null to "null" (empty val)
					// (as part of reclamation) *after* we performed our CAS above.
					// at any rate, we no longer need to help.
					
					// TODO: i forget now how this interacts with reclamation?
					//      need to re-conceptualize the algorithm in its entirety!
					// IIRC, op->newRoot can only transition from CASWORD(NULL) to CASWORD(node) to CASWORD_EMPTYVAL
					//      (the final state meaning the new root / subtree(?) was *reclaimed*)
					// QUESTION: how can this safely be reclaimed while we have a pointer to it? shouldn't EBR stop this?
					
					assert(IS_DIRTY_STARTED(op->parent->dirty));
					return NODE_TO_CASWORD(NULL);
				}
			}
		}
		assert(word != NODE_TO_CASWORD(NULL));
		assert(op->newRoot != NODE_TO_CASWORD(NULL));
		/* as per above, rebuildop was part of a subtree that was rebuilt, and "word" was reclaimed! */
		assert(op->newRoot == word || EMPTY_VAL_TO_CASWORD);
	    
		// stop here if there is no subtree to build (just one kvpair or node)
		if (IS_KVPAIR(word) || keyCount <= MAX_ACCEPTABLE_LEAF_SIZE) return word;

		assert(IS_NODE(word));
		Node *node = CASWORD_TO_NODE(word);
		assert(node->degree == numChildren);

		// opportunistically try to build different subtrees from any other concurrent threads
		// by synchronizing via node->capacity. concurrent threads increment node->capacity using cas
		// to "reserve" a subtree to work on (not truly exclusively---still a lock-free mechanism).
		while (1) {
			auto ix = node->capacity;
			// skip to the helping phase if all subtrees are already being constructed
			if (ix >= node->degree) break;
			// use cas to soft-reserve a subtree to construct
			if (__sync_bool_compare_and_swap(&node->capacity, ix, 1+ix))
				subtreeBuildAndReplace(tid, op, node, ix, childSize, remainder);
		}

		// try to help complete subtree building if necessary
		// (partially for lock-freedom, and partially for performance)

		// help linearly starting at a random position (to probabilistically scatter helpers)
		// TODO: determine if helping starting at my own thread id would help? or randomizing my chosen subtree every time i want to help one? possibly help according to a random permutation?
		auto ix = threadRNGs[tid].next(numChildren);
		for (size_t __i=0; __i<numChildren; ++__i) {
			auto i = (__i+ix) % numChildren;
			if (prov->readPtr(tid, node->ptrAddr(i)) == NODE_TO_CASWORD(NULL))
				subtreeBuildAndReplace(tid, op, node, i, childSize, remainder);
		}
	
		node->initSize = keyCount;
		node->minKey = node->key(0);
		node->maxKey = node->key(node->degree-2);
		assert(node->minKey != INF_KEY);
		assert(node->maxKey != INF_KEY);
		assert(node->minKey <= node->maxKey);
		return word;
	}

	void addKVPairsSubset(const int tid, RebuildOperation *op, Node *node,
	                      size_t *numKeysToSkip, size_t *numKeysToAdd,
	                      size_t depth, IdealBuilder *b,
	                      casword_t volatile *constructingSubtree)
	{
		for (size_t i=0;i<node->degree;++i) {
			if (*constructingSubtree != NODE_TO_CASWORD(NULL))
				return; // stop early if someone else built the subtree already
			
			assert(*numKeysToAdd > 0);
			assert(*numKeysToSkip >= 0);
			auto childptr = prov->readPtr(tid, node->ptrAddr(i));
			if (IS_VAL(childptr)) {
				if (IS_EMPTY_VAL(childptr))
					continue;
				
				if (*numKeysToSkip > 0) {
					--*numKeysToSkip;
				} else {
					assert(*numKeysToSkip == 0);
					auto v = CASWORD_TO_VAL(childptr);
					assert(i > 0);
					auto k = node->key(i - 1); // it's okay that this read is not atomic with the value read, since keys of nodes do not change. (so, we can linearize the two reads when we read the value.)
					b->addKV(tid, k, v);
					if (--*numKeysToAdd == 0) return;
				}
			} else if (IS_KVPAIR(childptr)) {
				if (*numKeysToSkip > 0) {
					--*numKeysToSkip;
				} else {
					assert(*numKeysToSkip == 0);
					auto pair = CASWORD_TO_KVPAIR(childptr);
					b->addKV(tid, pair->k, pair->v);
					if (--*numKeysToAdd == 0) return;
				}
			} else if (IS_REBUILDOP(childptr)) {
				auto child = CASWORD_TO_REBUILDOP(childptr)->rebuildRoot;
				assert(IS_DIRTY_FINISHED(child->dirty));
				auto childSize = DIRTY_FINISHED_TO_SUM(child->dirty);
				if (*numKeysToSkip < childSize) {
					addKVPairsSubset(tid, op, child, numKeysToSkip, numKeysToAdd, 1+depth, b, constructingSubtree);
					if (*numKeysToAdd == 0) return;
				} else {
					*numKeysToSkip -= childSize;
				}
			} else {
				assert(IS_NODE(childptr));
				auto child = CASWORD_TO_NODE(childptr);
				assert(IS_DIRTY_FINISHED(child->dirty));
				auto childSize = DIRTY_FINISHED_TO_SUM(child->dirty);
				if (*numKeysToSkip < childSize) {
					addKVPairsSubset(tid, op, child, numKeysToSkip, numKeysToAdd, 1+depth, b, constructingSubtree);
					if (*numKeysToAdd == 0) return;
				} else {
					*numKeysToSkip -= childSize;
				}
			}
		}
	}

	void subtreeBuildAndReplace(const int tid, RebuildOperation *op, Node *parent,
	                            size_t ix, size_t childSize, size_t remainder)
	{
		// compute initSize of new subtree
		auto totalSizeSoFar = ix*childSize + (ix < remainder ? ix : remainder);
		auto newChildSize = childSize + (ix < remainder);
		
		// build new subtree
		IdealBuilder b(this, newChildSize, 1+op->depth);
		auto numKeysToSkip = totalSizeSoFar;
		auto numKeysToAdd = newChildSize;

		// construct the subtree
		addKVPairsSubset(tid, op, op->rebuildRoot, &numKeysToSkip, &numKeysToAdd,
		                 op->depth, &b, parent->ptrAddr(ix)); 
		if (parent->ptr(ix) != NODE_TO_CASWORD(NULL)) return;
		
		auto ptr = b.getCASWord(tid, parent->ptrAddr(ix));
		// if we didn't build a tree, because someone else already replaced this
		// subtree, then we just stop here (just avoids an unnecessary cas below
		// in this case; apart from this cas, which will fail, the behaviour is no
		// different whether we return here or execute the following...)
		if (NODE_TO_CASWORD(NULL) == ptr) return; 
		
		// try to attach new subtree
		if (ix > 0) *parent->keyAddr(ix-1) = b.getMinKey();
		// try to CAS the subtree in to the new root we are building (consensus to decide who built it)
		if (!__sync_bool_compare_and_swap(parent->ptrAddr(ix), NODE_TO_CASWORD(NULL), ptr))
			freeSubtree(tid, ptr, false);
		assert(prov->readPtr(tid, parent->ptrAddr(ix)));
	}

	void freeNode(const int tid, Node *node, bool retire)
	{
//	if (retire) {
//		#ifndef IST_DISABLE_MULTICOUNTER_AT_ROOT
//		if (node->externalChangeCounter)
//			recordmgr->retire(tid, node->externalChangeCounter);
//		#endif
//		recordmgr->retire(tid, node);
//	} else {
//		#ifndef IST_DISABLE_MULTICOUNTER_AT_ROOT
//		if (node->externalChangeCounter)
//			recordmgr->deallocate(tid, node->externalChangeCounter);
//		#endif
//		recordmgr->deallocate(tid, node);
//	}
	}


	void freeSubtree(const int tid, casword_t ptr, bool retire)
	{
//		if (IS_KVPAIR(ptr)) {
////			if (retire)
////				recordmgr->retire(tid, CASWORD_TO_KVPAIR(ptr));
////			else
////				recordmgr->deallocate(tid, CASWORD_TO_KVPAIR(ptr));
//		} else if (IS_REBUILDOP(ptr)) {
//			auto op = CASWORD_TO_REBUILDOP(ptr);
//			freeSubtree(tid, NODE_TO_CASWORD(op->rebuildRoot), retire);
////			if (retire)
////				recordmgr->retire(tid, op);
////			else
////				recordmgr->deallocate(tid, op);
//		} else if (IS_NODE(ptr) && ptr != NODE_TO_CASWORD(NULL)) {
//			auto node = CASWORD_TO_NODE(ptr);
//			for (size_t i=0;i<node->degree;++i) {
//				auto child = prov->readPtr(tid, node->ptrAddr(i));
//				freeSubtree(tid, child, retire);
//			}
//			freeNode(tid, node, retire);
//		}
	}

	void helpFreeSubtree(const int tid, Node *node)
	{
//		// if node is the root of a *large* subtree (256+ children),
//		// then have threads *collaborate* by reserving individual subtrees to free.
//		// idea: reserve a subtree before freeing it by CASing it to NULL
//		//       we are done when all pointers are NULL.
//
//		// conceptually you reserve the right to reclaim everything under a node
//		// (including the node) when you set its DIRTY_DIRTY_MARKED_FOR_FREE_MASK bit
//		//
//		// note: the dirty field doesn't exist for kvpair, value, empty value and rebuildop objects...
//		// so to reclaim those if they are children of the root node passed to this function,
//		// we claim the entire root node at the end, and go through those with one thread.
//	    
//		// first, claim subtrees rooted at CHILDREN of this node
//		// TODO: does this improve if we scatter threads in this iteration?
//		for (size_t i=0;i<node->degree;++i) {
//			auto ptr = prov->readPtr(tid, node->ptrAddr(i));
//			if (IS_NODE(ptr)) {
//				Node *child = CASWORD_TO_NODE(ptr);
//				if (child == NULL) continue;
//				
//				// claim subtree rooted at child
//				while (true) {
//					auto old = child->dirty;
//					if (IS_DIRTY_MARKED_FOR_FREE(old)) break;
//					if (CASB(&child->dirty, old, old | DIRTY_MARKED_FOR_FREE_MASK))
//						freeSubtree(tid, ptr, true);
//				}
//			}
//		}
//	    
//		// then try to claim the node itself to handle special object types (kvpair, value, empty value, rebuildop).
//		// claim node and its pointers that go to kvpair, value, empty value and rebuildop objects, specifically
//		// (since those objects, and their descendents in the case of a rebuildop object,
//		// are what remain unfreed [since all descendents of direct child *node*s have all been freed])
//		while (true) {
//			auto old = node->dirty;
//			if (IS_DIRTY_MARKED_FOR_FREE(old)) break;
//			if (CASB(&node->dirty, old, old | DIRTY_MARKED_FOR_FREE_MASK)) {
//				// clean up pointers to non-*node* objects (and descendents of such objects)
//				for (size_t i=0;i<node->degree;++i) {
//					auto ptr = prov->readPtr(tid, node->ptrAddr(i));
//					if (!IS_NODE(ptr))
//						freeSubtree(tid, ptr, true);
//				}
//				freeNode(tid, node, true); // retire the ACTUAL node
//			}
//		}
	}

	void addKVPairs(const int tid, casword_t ptr, IdealBuilder *b)
	{
		if (IS_KVPAIR(ptr)) {
			KVPair *pair = CASWORD_TO_KVPAIR(ptr);
			b->addKV(tid, pair->k, pair->v);
		} else if (IS_REBUILDOP(ptr)) {
			RebuildOperation *op = CASWORD_TO_REBUILDOP(ptr);
			addKVPairs(tid, NODE_TO_CASWORD(op->rebuildRoot), b);
		} else {
			assert(IS_NODE(ptr));
			Node *node = CASWORD_TO_NODE(ptr);
			assert(IS_DIRTY_FINISHED(node->dirty) && IS_DIRTY_STARTED(node->dirty));
			for (size_t i=0;i<node->degree;++i) {
				auto childptr = prov->readPtr(tid, node->ptrAddr(i));
				if (IS_VAL(childptr)) {
					if (IS_EMPTY_VAL(childptr)) continue;
					auto v = CASWORD_TO_VAL(childptr);
					assert(i > 0);
					// it's okay that this read is not atomic with the value read,
					// since keys of nodes do not change. (so, we can linearize the
					// two reads when we read the value.)
					auto k = node->key(i - 1); 
					b->addKV(tid, k, v);
				} else {
					addKVPairs(tid, childptr, b);
				}
			}
		}
	}
	/*************************************************************************/
};

#define IST_BROWN_TEMPL template<typename K, typename V>
#define IST_BROWN_FUNCT ist_brown<K,V>

IST_BROWN_TEMPL
bool IST_BROWN_FUNCT::contains(const int tid, const K& key)
{
	const V ret = lookup_helper(tid, key);
	return (ret != this->NO_VALUE);
}

IST_BROWN_TEMPL
const std::pair<V,bool> IST_BROWN_FUNCT::find(const int tid, const K& key)
{
	const V ret = lookup_helper(tid, key);
	return std::pair<V,bool>(ret, ret != this->NO_VALUE);
}

IST_BROWN_TEMPL
int IST_BROWN_FUNCT::rangeQuery(const int tid, const K& lo, const K& hi,
                      std::vector<std::pair<K,V>> kv_pairs)
{
	return 0;
}

IST_BROWN_TEMPL
const V IST_BROWN_FUNCT::insert(const int tid, const K& key, const V& val)
{
	return doUpdate(tid, key, val, InsertReplace);
}

IST_BROWN_TEMPL
const V IST_BROWN_FUNCT::insertIfAbsent(const int tid, const K& key, const V& val)
{
	return doUpdate(tid, key, val, InsertIfAbsent);
}

IST_BROWN_TEMPL
const std::pair<V,bool> IST_BROWN_FUNCT::remove(const int tid, const K& key)
{
	V ret = doUpdate(tid, key, this->NO_VALUE, Erase);
	return std::pair<V,bool>(ret, ret != this->NO_VALUE);
}

IST_BROWN_TEMPL
bool IST_BROWN_FUNCT::validate()
{
	return validate_helper();
}
