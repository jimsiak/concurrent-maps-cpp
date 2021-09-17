#pragma once

#include <cstring>

#include "../map_if.h"
#include "cg_sync_htm.h"
#include "cg_sync_locks.h"

template <class K, class V>
class cg_ds : public Map<K, V> {
public:

	cg_ds(const K _NO_KEY, const V _NO_VALUE, const int numProcesses, Map<K,V> *prot)
	{
		protected_data_structure = prot;
		sync_mechanism = new cg_sync_htm();
//		sync_mechanism = new cg_sync_rwlock();
//		sync_mechanism = new cg_sync_spinlock();
	}

	~cg_ds()
	{
		delete protected_data_structure;
	}

	void initThread(const int tid)
	{
		protected_data_structure->initThread(tid);
	}

	void deinitThread(const int tid)
	{
		protected_data_structure->deinitThread(tid);
	}

	bool contains(const int tid, const K& key)
	{
		bool ret;
		sync_mechanism->cs_enter_ro();
		ret = protected_data_structure->contains(tid, key);
		sync_mechanism->cs_exit();
		return ret;
	}

	const std::pair<V,bool> find(const int tid, const K& key)
	{
		std::pair<V, bool> ret;
		sync_mechanism->cs_enter_ro();
		ret = protected_data_structure->find(tid, key);
		sync_mechanism->cs_exit();
		return ret;
	}

	int rangeQuery(const int tid, const K& lo, const K& hi,
	               std::vector<std::pair<K,V>> kv_pairs)
	{
		int ret;
		sync_mechanism->cs_enter_ro();
		ret = protected_data_structure->rangeQuery(tid, lo, hi, kv_pairs);
		sync_mechanism->cs_exit();
		return ret;
	}

	const V insert(const int tid, const K& key, const V& val)
	{
		V ret;
		sync_mechanism->cs_enter_rw();
		ret = protected_data_structure->insert(tid, key, val);
		sync_mechanism->cs_exit();
		return ret;
	}

	const V insertIfAbsent(const int tid, const K& key, const V& val)
	{
		V ret;
		sync_mechanism->cs_enter_rw();
		ret = protected_data_structure->insertIfAbsent(tid, key, val);
		sync_mechanism->cs_exit();
		return ret;
	}

	const std::pair<V,bool> remove(const int tid, const K& key)
	{
		std::pair<V, bool> ret;
		sync_mechanism->cs_enter_rw();
		ret = protected_data_structure->remove(tid, key);
		sync_mechanism->cs_exit();
		return ret;
	}

	bool validate()
	{
		return protected_data_structure->validate();
	}

	char *name()
	{
		char *baseds = protected_data_structure->name();
		char *name = new char[60];
		strcpy(name, baseds);
		strcat(name, " (");
		strcat(name, sync_mechanism->name());
		strcat(name, ")");
		return name;
	}

private:

	Map<K,V> *protected_data_structure;
	cg_sync *sync_mechanism;

};
