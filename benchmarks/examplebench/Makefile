GPP = g++

OPT_LEVEL ?= -O3
GDB_SYMBOLS ?= -g
INCFLAG = -I../../lib
STDFLAG = -std=c++11
MCXFLAG = -mcx16
PTHREADFLAG = -pthread
RTMFLAG = -mrtm

WARNINGS = -Wall -Wextra -Wno-write-strings -Wno-unused-parameter -Wno-unused-but-set-variable -Wno-unused-variable -Wno-ignored-qualifiers -Wno-sequence-point -Wno-array-bounds

GPPFLAGS = $(WARNINGS) $(OPT_LEVEL) $(GDB_SYMBOLS) $(INCFLAG) $(STDFLAG) $(PTHREADFLAG) $(RTMFLAG) $(MCXFLAG)

all: clean x.examplebench

x.examplebench: main.cpp
	$(GPP) $(GPPFLAGS) $(PARAMFLAGS) $^ -o $@

clean: 
	rm -f x.examplebench
