#pragma once

#include <pthread.h>

#include "cg_sync_if.h"

class cg_sync_spinlock : public cg_sync {
public:

	cg_sync_spinlock()
	{
		pthread_spin_init(&lock, PTHREAD_PROCESS_SHARED);
	}

	void cs_enter_rw() { pthread_spin_lock(&lock); }
	void cs_exit() { pthread_spin_unlock(&lock); }

	char *name() { return (char *)"CG-SPINLOCK"; }

private:

	pthread_spinlock_t lock;
};

class cg_sync_rwlock : public cg_sync {
public:

	cg_sync_rwlock()
	{
		pthread_rwlock_init(&lock, NULL);
	}

	void cs_enter_rw() { pthread_rwlock_wrlock(&lock); }
	void cs_enter_ro() { pthread_rwlock_rdlock(&lock); }
	void cs_exit() { pthread_rwlock_unlock(&lock); }

	char *name() { return (char *)"CG-RWLOCK"; }

private:

	pthread_rwlock_t lock;
};
