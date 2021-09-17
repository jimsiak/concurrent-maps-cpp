#pragma once

typedef pthread_spinlock_t node_lock_t;
#define INIT_LOCK(lock) pthread_spin_init(lock, PTHREAD_PROCESS_SHARED)
#define LOCK(lock)      pthread_spin_lock(lock)
#define UNLOCK(lock)    pthread_spin_unlock(lock)
#define TRYLOCK(lock)   pthread_spin_trylock(lock)
