#CC = g++
CC = /various/common_tools/gcc-5.3.0/bin/g++

CFLAGS = -std=c++14 -O3 -g
CFLAGS += -DMAX_THREADS_POW2=256 -DCPU_FREQ_GHZ=2.1
CFLAGS += $(xargs)
CFLAGS += -Wno-format
CFLAGS += -DNO_CLEANUP_AFTER_WORKLOAD
CFLAGS += -mrtm -mcx16
CFLAGS += -Wno-write-strings
CFLAGS += -fno-omit-frame-pointer
CFLAGS += -DALIGNED_ALLOCATIONS

### if you do not have PAPI, comment out these two lines
#CFLAGS += -DUSE_PAPI
#LDFLAGS += -lpapi
#LDFLAGS += -lnuma

SRC_DIRS = ./ ./benchmarks/ ./concurrency_control/ ./storage/ ./storage/index/ ./system/ 
CFLAGS += $(patsubst %,-I%,$(SRC_DIRS))
CFLAGS += -I../../lib/ -I../../lib/papi/

LDFLAGS = $(CFLAGS)
LDFLAGS += -lpthread

CPPS = $(foreach dir, $(SRC_DIRS), $(wildcard $(dir)*.cpp))

all: x.macrobench.TPCC x.macrobench.YCSB

x.macrobench.TPCC: $(CPPS)
	$(CC) -o $@ $^ $(LDFLAGS) -DWORKLOAD=TPCC
x.macrobench.YCSB: $(CPPS)
	$(CC) -o $@ $^ $(LDFLAGS) -DWORKLOAD=YCSB

clean:
	rm -f x.macrobench.*
