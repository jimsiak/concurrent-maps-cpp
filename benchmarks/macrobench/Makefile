#CC = g++
CC = /various/common_tools/gcc-5.3.0/bin/g++

CFLAGS = -std=c++14 -O3 -g
CFLAGS += -DMAX_THREADS_POW2=256 -DCPU_FREQ_GHZ=2.1
CFLAGS += -DALIGNED_ALLOCATIONS
CFLAGS += $(xargs)
CFLAGS += -Wno-format
CFLAGS += -mrtm -mcx16
CFLAGS += -Wno-write-strings
CFLAGS += -fno-omit-frame-pointer

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
OBJDIR = OBJS
OBJS_TPCC = $(foreach obj, $(CPPS:.cpp=.o), $(OBJDIR)/TPCC/$(obj))
OBJS_YCSB = $(foreach obj, $(CPPS:.cpp=.o), $(OBJDIR)/YCSB/$(obj))
dir_guard=@mkdir -p $(@D)

all: x.macrobench.TPCC x.macrobench.YCSB

x.macrobench.TPCC: $(OBJS_TPCC)
	$(CC) -o $@ $^ $(LDFLAGS) -DWORKLOAD=TPCC
x.macrobench.YCSB: $(OBJS_YCSB)
	$(CC) -o $@ $^ $(LDFLAGS) -DWORKLOAD=YCSB

$(OBJDIR)/TPCC/%.o: %.cpp
	$(dir_guard)
	$(CC) -c $(CFLAGS) -DWORKLOAD=TPCC -o $@ $<

$(OBJDIR)/YCSB/%.o: %.cpp
	$(dir_guard)
	$(CC) -c $(CFLAGS) -DWORKLOAD=YCSB -o $@ $<

clean:
	rm -rf $(OBJDIR) x.macrobench.*
