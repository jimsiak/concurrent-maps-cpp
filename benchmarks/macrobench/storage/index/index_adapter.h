/* 
 * File:   index_with_rq.h
 * Author: trbot
 *
 * Created on May 28, 2017, 3:03 PM
 */

#ifndef INDEX_WITH_RQ_H
#define INDEX_WITH_RQ_H

#include <limits>
#include <csignal>
#include <cstring>
#include "row.h"
#include "row_lock.h"
#include "index_base.h"     // for table_t declaration, and parent class inheritance

#include <ctime>
//#include "adapter.h"
#include "../../ds/map_factory.h"

//> Instead of "#include plaf.h"
#ifndef MAX_THREADS_POW2
    #define MAX_THREADS_POW2 128 // MUST BE A POWER OF TWO, since this is used for some bitwise operations
#endif

/**
 * Create an adapter class for the DBx1000 index interface
 */
class Index : public index_base {
private:
//    ds_adapter<KEY_TYPE, VALUE_TYPE> * index;
	Map<KEY_TYPE, VALUE_TYPE> *index;
    
public:
    
    ~Index() {
        std::cout<<"  performing delete"<<std::endl;
        delete index;
    }
    
    // WARNING: DO NOT OVERLOAD init() WITH NO ARGUMENTS!!!
    RC init(uint64_t part_cnt, table_t * table) {
//        if (part_cnt != 1) setbench_error("part_cnt != 1 unsupported");
        if (part_cnt != 1) std::cout << "part_cnt != 1 unsupported\n";

        KEY_TYPE minKey = __NO_KEY;
        KEY_TYPE maxKey = std::numeric_limits<KEY_TYPE>::max();
        VALUE_TYPE reservedValue = __NO_VALUE;
        
        if (g_thread_cnt > MAX_THREADS_POW2) {
//            setbench_error("g_thread_cnt > MAX_THREADS_POW2");
			std::cout << "g_thread_cnt > MAX_THREADS_POW2\n";
        }

//        index = new ds_adapter<KEY_TYPE, VALUE_TYPE>(MAX_THREADS_POW2, minKey, maxKey, reservedValue, NULL);
		std::string map_type("treap-seq");
		std::string sync_type("cg-sync");
		index = createMap<KEY_TYPE,VALUE_TYPE>(map_type, sync_type);
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

//            if (oldItem != index->getNoValue()) {
//                // adding to existing list
//                newItem->next = oldItem->next;
//                oldItem->next = newItem;
//            }
        unlock_key(key);
#endif
        INCREMENT_NUM_INSERTS(tid);
        return RCOK;
    }
    RC index_read(KEY_TYPE key, VALUE_TYPE * item, int part_id = -1, int thd_id = 0) {
//        lock_key(key);
//            *item = (VALUE_TYPE) index->find(tid, key);
		std::pair<VALUE_TYPE, bool> ret;
		ret = index->find(tid, key);
		*item = ret.first;
//        unlock_key(key);
        INCREMENT_NUM_READS(tid);
        return RCOK;
    }
    // finds all keys in the set in [low, high],
    // saves the number N of keys in numResults,
    // saves the keys themselves in resultKeys[0...N-1],
    // and saves their values in resultValues[0...N-1].
    RC index_range_query(KEY_TYPE low, KEY_TYPE high, KEY_TYPE * resultKeys, VALUE_TYPE * resultValues, int * numResults, int part_id = -1) {
//        *numResults = index->rangeQuery(tid, low, high, resultKeys, (VALUE_TYPE *) resultValues);
        INCREMENT_NUM_RQS(tid);
        return RCOK;
    }
    void initThread(const int tid) {
        index->initThread(tid);
    }
    void deinitThread(const int tid) {
        index->deinitThread(tid);
    }
    
    size_t getNodeSize() {
//        return sizeof(NODE_TYPE);
        return 0;
    }
    
    size_t getDescriptorSize() {
//        return sizeof(DESCRIPTOR_TYPE);
        return 0;
    }
    
    void print_stats(){
//        index->printObjectSizes();
//        index->printSummary();
    }
};

#endif /* INDEX_WITH_RQ_H */
