/* 
 * File:   all_indexes.h
 * Author: trbot
 *
 * Created on May 28, 2017, 4:33 PM
 */

#pragma once

#include "config.h"

#if defined IDX_HASH
#   include "index_hash.h"
#else
#   include "index_adapter.h"
#endif
