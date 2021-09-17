#pragma once 

#if defined(MAP_KEY_TYPE_INT)
#	include "key_int.h"
#elif defined(MAP_KEY_TYPE_ULLONG)
#	include "key_ullong.h"
#elif defined (MAP_KEY_TYPE_BIG_INT)
#	ifndef BIG_INT_KEY_SZ
#		define BIG_INT_KEY_SZ 50
#	endif
#	include "key_big_int.h"
#elif defined (MAP_KEY_TYPE_STR)
#	ifndef STR_KEY_SZ
#		define STR_KEY_SZ 50
#	endif
#	include "key_str.h"
#else
#	error "No key type defined..."
#endif
