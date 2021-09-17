#pragma once

#include <stdio.h>
#include <stdarg.h>

typedef enum {
	LVL_DEBUG = 0,
	LVL_INFO,
	LVL_WARNING,
	LVL_ERROR,
	LVL_CRITICAL
} log_level_t;

//> Default LOG_LVL
#ifndef LOG_LVL
#	define LOG_LVL LVL_INFO
#endif

#define PRINT_MSG_PREFIX(lvl) \
//	printf("%10s - %s:%04d - %s ==> ", #lvl, __FILE__, __LINE__, __func__)
//	printf("%10s - %15s() ==> ", #lvl, __func__)

#define log_print(lvl, fmt, ...) \
	do { \
		if ((lvl) >= LOG_LVL) { \
			PRINT_MSG_PREFIX(lvl); \
			printf((fmt), ##__VA_ARGS__); \
		} \
	} while(0) \

#if (LVL_DEBUG >= LOG_LVL)
#	define log_debug(fmt, ...) log_print(LVL_DEBUG, (fmt), ##__VA_ARGS__)
#else
#	define log_debug(fmt, ...)
#endif

#if (LVL_INFO >= LOG_LVL)
#	define log_info(fmt, ...) log_print(LVL_INFO, (fmt), ##__VA_ARGS__)
#else
#	define log_info(fmt, ...)
#endif

#if (LVL_WARNING >= LOG_LVL)
#	define log_warning(fmt, ...) log_print(LVL_WARNING, (fmt), ##__VA_ARGS__)
#else
#	define log_warning(fmt, ...)
#endif

#if (LVL_ERROR >= LOG_LVL)
#	define log_error(fmt, ...) log_print(LVL_ERROR, (fmt), ##__VA_ARGS__)
#else
#	define log_error(fmt, ...)
#endif

#if (LVL_CRITICAL >= LOG_LVL)
#	define log_critical(fmt, ...) log_print(LVL_CRITICAL, (fmt), ##__VA_ARGS__)
#else
#	define log_critical(fmt, ...)
#endif
