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

### Lock-based

### Lock-free

### RCU and HTM based

### RCU-HTM based
