#GPP = g++
GPP = /various/common_tools/gcc-5.3.0/bin/g++

#KEY_TYPE = MAP_KEY_TYPE_ULLONG
KEY_TYPE = MAP_KEY_TYPE_INT

PARAMFLAGS = -D$(KEY_TYPE)

OPT_LEVEL ?= -O3
GDB_SYMBOLS ?= -g
INCFLAG = -I../../lib
STDFLAG = -std=c++11
MCXFLAG = -mcx16
PTHREADFLAG = -pthread
RTMFLAG = -mrtm

WARNINGS = -Wall -Wextra -Wno-write-strings -Wno-unused-parameter -Wno-unused-but-set-variable -Wno-unused-variable -Wno-ignored-qualifiers -Wno-sequence-point -Wno-array-bounds

GPPFLAGS = $(WARNINGS) $(OPT_LEVEL) $(GDB_SYMBOLS) $(INCFLAG) $(STDFLAG) $(PTHREADFLAG) $(RTMFLAG) $(MCXFLAG)

all: clean x.microbench

x.microbench: bench.cpp
	$(GPP) $(GPPFLAGS) $(PARAMFLAGS) $^ -o $@

clean: 
	rm -f x.microbench
