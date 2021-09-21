#include <cassert>
#include <string>
#include "../../ds/map_factory.h"

int main(int argc, char **argv)
{
	const int tid = 0; //> Single thread.

	if (argc != 2) {
		fprintf(stderr, "usage: %s <map-type>\n", argv[0]);
		return 1;
	}

	std::string map_type(argv[1]);
	std::string sync_type("");
	Map<int, void*> *mapds = createMap<int, void*>(map_type, sync_type);

	printf("map-type: %s\n", argv[1]);

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

