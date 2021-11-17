#GPP = g++
GPP = /various/common_tools/gcc-5.3.0/bin/g++

OPT_LEVEL ?= -O3
GDB_SYMBOLS ?= -g
INCFLAG = -I../../lib
STDFLAG = -std=c++11
MCXFLAG = -mcx16
PTHREADFLAG = -pthread
RTMFLAG = -mrtm

WARNINGS = -Wall -Wextra -Wno-write-strings -Wno-unused-parameter -Wno-unused-but-set-variable -Wno-unused-variable -Wno-ignored-qualifiers -Wno-sequence-point -Wno-array-bounds

GPPFLAGS = $(WARNINGS) $(OPT_LEVEL) $(GDB_SYMBOLS) $(INCFLAG) $(STDFLAG) $(PTHREADFLAG) $(RTMFLAG) $(MCXFLAG)

.PHONY: all clean x.microbench.*

all: clean x.microbench.ullong x.microbench.cppullong8

x.microbench.ullong: bench.cpp
	$(GPP) $(GPPFLAGS) $^ -o $@ -DMAP_KEY_TYPE_ULLONG
x.microbench.cppullong%: bench.cpp
	$(GPP) $(GPPFLAGS) $^ -o $@ -DMAP_KEY_TYPE_CPPULLONG -DCPPULLONG_KEY_SZ=$*

clean: 
	rm -f x.microbench.*
