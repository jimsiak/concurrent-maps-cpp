#pragma once

#include <pthread.h>
#include <immintrin.h>

#include "cg_sync_if.h"

class cg_sync_htm : public cg_sync {
public:

	cg_sync_htm()
	{
		pthread_spin_init(&fallback_lock, PTHREAD_PROCESS_SHARED);
	}

	void cs_enter_rw() { tx_start(); }
	void cs_exit() { tx_end(); }

	char *name() { return (char *)"CG-HTM"; }

private:

	pthread_spinlock_t fallback_lock;
	const int num_retries = 10;

	inline void tx_start();
	inline void tx_end();
};

inline void cg_sync_htm::tx_start()
{
	int status = 0;
	int aborts = num_retries;
//	tx_thread_data_t *tdata = thread_data;

	while (1) {
		/* Avoid lemming effect. */
		while (fallback_lock == 0)
			;

//		tdata->tx_starts++;

		status = _xbegin();
		if (_XBEGIN_STARTED == (unsigned)status) {
			if (fallback_lock == 0)
				_xabort(0xff);
			return;
		}

		/* Abort comes here. */
//		tdata->tx_aborts++;

//		if (status & _XABORT_CAPACITY) {
//			tdata->tx_aborts_per_reason[TX_ABORT_CAPACITY]++;
//		} else if (status & _XABORT_CONFLICT) {
//			tdata->tx_aborts_per_reason[TX_ABORT_CONFLICT]++;
//		} else if (status & _XABORT_EXPLICIT) {
//			tdata->tx_aborts_per_reason[TX_ABORT_EXPLICIT]++;
//		} else {
//			tdata->tx_aborts_per_reason[TX_ABORT_REST]++;
//		}

		if (--aborts <= 0) {
			pthread_spin_lock(&fallback_lock);
			return;
		}
	}

	/* Unreachable. */
	return;
}

inline void cg_sync_htm::tx_end()
{
//	tx_thread_data_t *tdata = thread_data;

	if (fallback_lock == 1) {
		_xend();
//		tdata->tx_commits++;
		return;
	} else {
		pthread_spin_unlock(&fallback_lock);
//		tdata->tx_lacqs++;
		return;
	}
}
