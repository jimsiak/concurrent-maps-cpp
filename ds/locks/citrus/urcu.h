/**
 * Copyright 2014 Maya Arbel (mayaarl [at] cs [dot] technion [dot] ac [dot] il).
 * 
 * This file is part of Citrus. 
 * 
 * Citrus is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * Authors Maya Arbel and Adam Morrison 
 */

#pragma once

#if !defined(EXTERNAL_RCU)

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

typedef struct rcu_node_t {
    volatile long time; 
    char p[184];
} rcu_node;

static void initURCU(int num_threads);
static void urcu_read_lock();
static void urcu_read_unlock();
static void urcu_synchronize(); 
static void urcu_register(int id);
static void urcu_unregister();

static int threads; 
static rcu_node **urcu_table;

static __thread volatile long *times = NULL; 
static __thread int i; 
static __thread int tid;

static void initURCU(int num_threads)
{
   rcu_node **result = (rcu_node**)malloc(sizeof(rcu_node)*num_threads);

   rcu_node *new_node;
   threads = num_threads; 
   for(int i = 0; i < threads; i++){
        new_node = (rcu_node *)malloc(sizeof(rcu_node));
        new_node->time = 1; 
        *(result + i) = new_node;
    }
    urcu_table =  result;
    printf("initializing URCU finished, node_size: %zd\n", sizeof(rcu_node));
    return; 
}

static void urcu_register(int id)
{
    times = (long *)malloc(sizeof(long)*threads);
    i = id; 
	tid = id;
    if (times == NULL) {
        printf("malloc failed\n");
        exit(1);
    }
}

static void urcu_unregister()
{
    free((void *)times);
}

static void urcu_read_lock()
{
    assert(urcu_table[i] != NULL);
    __sync_add_and_fetch(&urcu_table[i]->time, 1);
}

static inline void set_bit(int nr, volatile long *addr)
{
    asm("btsl %1,%0" : "+m" (*addr) : "Ir" (nr));
}

static void urcu_read_unlock()
{
    assert(urcu_table[i] !=  NULL);
    set_bit(0, &urcu_table[i]->time);
}

static void urcu_synchronize()
{
    //read old counters
    for(int i=0; i<threads ; i++) times[i] = urcu_table[i]->time;

    for(int i = 0; i < threads; i++){
		if (i == tid) continue;
        if (times[i] & 1) continue;
        while(1) {
            unsigned long t = urcu_table[i]->time;
            if (t & 1 || t > times[i]) break; 
        }
    }
}

#else

#include <urcu.h>
static inline void initURCU(int num_threads) { rcu_init(); }
static inline void urcu_register(int id) { rcu_register_thread(); }
static inline void urcu_unregister() { rcu_unregister_thread(); }
static inline void urcu_read_lock() { rcu_read_lock(); }
static inline void urcu_read_unlock() { rcu_read_unlock(); }
static inline void urcu_synchronize() { synchronize_rcu(); }

#endif
