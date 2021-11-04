/* 
 * File:   index_with_rq.h
 * Author: trbot
 *
 * Created on May 28, 2017, 3:03 PM
 */
#pragma once

#include <iomanip>
#include "table.h"
#include "index_base.h"
#include "../../ds/map_factory.h"

/**
 * Create an adapter class for the DBx1000 index interface
 */
class Index : public index_base {
private:
	Map<KEY_TYPE, VALUE_TYPE> *index;

public:
	~Index() { delete index; }

	// WARNING: DO NOT OVERLOAD init() WITH NO ARGUMENTS!!!
	RC init(uint64_t part_cnt, table_t * table) {
		if (part_cnt != 1) std::cout << "part_cnt != 1 unsupported\n";
		std::string map_type = g_params["data-structure"];
		std::string sync_type(g_params["sync-type"]);
		std::cout << "Initiating Map data structure as index for table "
		          << std::setw(12) << table->get_table_name() << " ... "
		          << std::flush;
		index = createMap<KEY_TYPE,VALUE_TYPE>(map_type, sync_type);
		std::cout << "[ type: " << index->name() << " ]\n";
		this->table = table;
		return RCOK;
	}

	RC index_insert(KEY_TYPE key, VALUE_TYPE newItem, int part_id = -1) {
		#if defined USE_RANGE_QUERIES
		auto oldVal = index->insertIfAbsent(tid, key, newItem);
		// TODO: determine if there are index collisions in anything but orderline
		//       (and determine why they happen in orderline, and if it's ok)
		#else
		newItem->next = NULL;
		lock_key(key);
		VALUE_TYPE oldItem = index->insertIfAbsent(tid, key, newItem);
		if (oldItem != NULL) {
			// adding to existing list
			newItem->next = oldItem->next;
			oldItem->next = newItem;
		}
		unlock_key(key);
		#endif
		INCREMENT_NUM_INSERTS(tid);
		return RCOK;
	}

	RC index_read(KEY_TYPE key, VALUE_TYPE * item, int part_id = -1,
	              int thd_id = 0) {
		std::pair<VALUE_TYPE, bool> ret;
		ret = index->find(tid, key);
		*item = ret.first;
		INCREMENT_NUM_READS(tid);
		return RCOK;
	}

	// finds all keys in the set in [low, high],
	// saves the number N of keys in numResults,
	// saves the keys themselves in resultKeys[0...N-1],
	// and saves their values in resultValues[0...N-1].
	RC index_range_query(KEY_TYPE low, KEY_TYPE high, KEY_TYPE * resultKeys,
	                     VALUE_TYPE * resultValues, int * numResults,
	                     int part_id = -1) {
//		*numResults = index->rangeQuery(tid, low, high, resultKeys, (VALUE_TYPE *) resultValues);
		INCREMENT_NUM_RQS(tid);
		return RCOK;
	}

	void initThread(const int tid) { index->initThread(tid); }
	void deinitThread(const int tid) { index->deinitThread(tid); }

	size_t getNodeSize() { return 0; }
	size_t getDescriptorSize() { return 0; }
	void print_stats(){}

	void print() { index->print(); }
	bool validate() { index->validate(); }
};
