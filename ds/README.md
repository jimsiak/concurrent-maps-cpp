# Concurrent Map Implementations in C++

:grinning: :grinning: :grinning: :grinning: :grinning:

This repo contains all the implementations of sequential and concurrent
data structures that implement the dictionary (a.k.a. map) ADT.

## An example usage of a Map data structure (single-thread)

```C
#include <cassert>
#include <string>
#include "../../ds/map_factory.h"

int main()
{
	const int tid = 0; //> Single thread.

	std::string map_type("bst-unb-ext");
	std::string sync_type("");
	Map<int, void*> *mapds = createMap<int, void*>(map_type, sync_type);

	mapds->initThread(tid);
	for (int i=0; i < 100; i++)
		mapds->insert(tid, i, (void *)i);

	bool ret = mapds->contains(tid, 10);
	assert(ret); // Key in the Map
	ret = mapds->contains(tid, 200);
	assert(!ret); // Key not in the Map

	std::pair<void *,bool> retp = mapds->remove(tid, 10);
	assert(retp.second && retp.first == (void *)10);
	ret = mapds->contains(tid, 10);
	assert(!ret); // Key not in the Map
	return 0;
}
```

## The interface of a Map data structure

The full interface of a Map data structure is found in `map_if.h` and I also
provide a function called `createMap()` in `map_factory.h` which is the
function used to initialize a tree given its type and the desired
synchronization mechanism to augment the data structure.

Briefly, a Map data structure provides the following basic operations:
* `contains(key)`: Returns `true` if key is found, `false` otherwise
* `find(key)`: Returns an `std::pair<V,bool>`, where the second argument is true of false if
  the key was found or not found, respectively, and the first argument is the
  value associated with the respective key. If the key was not present in the
  Map, this can be anything (FIXME: it should be NO_VALUE, but I need to decide
  how NO_VALUE will be passed from the benchmark to the data structure).
* `rangeQuery(key1, key2)`: Returns an `std::vector<std::pair<K,V>>` which contains all the key-value
  pairs with keys inside the range [key1, key2].
* `insert(key, value)`: Inserts the key-value pair in the tree, replacing the old pair if the key was
  already present in the Map. Returns the old value associated with the corresponding key.
* `insertIfAbsent(key, value)`: Inserts the key-value pair in the tree only if the key was not present in
  the tree previously. If the key was already present, the value associated with it is returned.
* `remove(key)`: Deletes the key-value pair with the given key from the Map data structure
  and returns an `std::pair<V,bool>` where the second argument indicates whether
  the key was found or not, and if found, the first argument is the value that
  was associated with it.


## Type of keys and values stored in a Map data structure

## Which data structures are currently implemented?

Currently the following data structures have been implemented:

### Sequential

* Unbalanced Binary Search Tree
  * Internal
  * Partially External
  * External
* AVL Binary Search Tree
  * Internal
  * Partially External
  * External
* Red-Black Binary Search Tree
  * Internal
  * External
* Treap Binary Search Tree
* B+-tree
* (a-b)-tree

### Coarse-grained synchronization mechanisms

All the above sequential data structures can be protected by any of the following coarse-grained synchronization mechanisms:

* cg-spin: enclose each operation in a pthread spinlock acquire/release section.
* cg-rwlock: enclose each operation in pthread rwlock acquire/release section.
* cg-htm: enclose each operation in an HTM transaction using Intel's TSX instructions.

### Lock-based

* Unbalanced External BST with hand-over-hand locking.
* Unbalanced Internal Citrus BST by Arbel et. al [[1]](#1).
* Relaxed-balance Partially External AVL BST by Bronson et. al [[2]](#2).
* Relaxed-balance Internal AVL BST by Drachsler et. al [[3]](#3).
* Relaxed-balance Partially External AVL BST by Crain et. al (a.k.a., Contention-friendly) [[4]](#4).
* Contention-adaptive Treap by Winblad et. al [[5]](#5).

### Lock-free

* Unbalanced Internal BST by Ellen et. al [[6]](#).
* Unbalanced Internal BST by Howley et. al [[7]](#7).
* Unbalanced External BST by Natarajan et. al [[#8]](#8).
* Unbalanced External BST with 3-path synchronization by Brown et. al [[9]](#9).
* Relaxed-balance (a-b)-tree with 3-path synchronization by Brown et. al [[9]](#9).
* Interpolation ST synchronized with Double-Compare-Single-Swap (DCSS) by Brown et. al [[10]](#10).
* OpenBW-tree by Wang et. al [[11]](#11).

### RCU and HTM based

* External AVL BST synchronized with Consistency-oblivious programming (COP) by Avni et. al [[12]](#12).
* Internal AVL BST synchronized with Consistency-oblivious programming (COP) by Avni et. al [[12]](#12)
* RCU with coarse-grained lock synchronization for updaters (RCU-SGL) for all the data structures for with an RCU-HTM version is provided.

### RCU-HTM based

* Unbalanced Binary Search Tree
  * Internal
  * Partially External
  * External
* AVL Binary Search Tree
  * Internal
  * Partially External
  * External
* B+-tree
* (a-b)-tree

## References
<a id="1">[1]</a> 
Arbel (2014). 
Concurrent updates with RCU: search tree as an example

<a id="2">[2]</a> 
Bronson (2010). 
A Practical Concurrent Binary Search Tree.

<a id="3">[3]</a> 
Drachsler (2014). 
Practical concurrent binary search trees via logical ordering.

<a id="4">[4]</a> 
Crain (2013). 
The Contention-Friendly Tree.

<a id="5">[5]</a> 
Winblad (2018). 
Lock-free Contention Adapting Search Trees

<a id="6">[6]</a> 
Ellen (2010). 
Non-blocking Binary Search Trees.

<a id="7">[7]</a> 
Howley (2012). 
A non-blocking internal binary search tree.

<a id="8">[8]</a> 
Crain (2014). 
Fast concurrent lock-free binary search trees.

<a id="9">[9]</a> 
Brown (2017). 
A Template for Implementing Fast Lock-free Trees Using HTM.

<a id="10">[10]</a> 
Brown (2020). 
Non-blocking interpolation search trees with doubly-logarithmic running time.

<a id="11">[11]</a> 
Wang (2018).
Building a Bw-Tree Takes More Than Just Buzz Words.

<a id="12">[12]</a>
Avni (2014).
Improving HTM Scaling with Consistency-Oblivious Programming.
