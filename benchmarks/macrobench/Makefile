CFLAGS =
LDFLAGS =
CC = g++

CFLAGS += -DMAX_THREADS_POW2=256
CFLAGS += -DCPU_FREQ_GHZ=2.1 #$(shell ../microbench/experiments/get_cpu_ghz.sh)
CFLAGS += -g -std=c++14 -O3
CFLAGS += $(xargs)
CFLAGS += -Wno-format
CFLAGS += -DNO_CLEANUP_AFTER_WORKLOAD
CFLAGS += -mrtm
CFLAGS += -Wno-write-strings
CFLAGS += -DALIGNED_ALLOCATIONS
CFLAGS += -fno-omit-frame-pointer

### if you do not have PAPI, comment out these two lines
#CFLAGS += -DUSE_PAPI
#LDFLAGS += -lpapi
#LDFLAGS += -lnuma

WORKLOAD ?= TPCC ## TPCC or YCSB
CFLAGS += -DWORKLOAD=$(WORKLOAD)

OBJ_DIR = OBJS
SRC_DIRS = ./ ./benchmarks/ ./concurrency_control/ ./storage/ ./storage/index/ ./system/ 
CFLAGS += -I../ $(patsubst %,-I%,$(SRC_DIRS))
CFLAGS += `find ../../../../trevor_brown_ppopp20_interpolation_trees/common -type d | sed s/^/-I/`
CFLAGS += -I../../lib/

LDFLAGS += $(CFLAGS)
LDFLAGS += -L. -L../lib -g
LDFLAGS += -lpthread

CPPS = $(foreach dir, $(SRC_DIRS), $(wildcard $(dir)*.cpp))
OBJS = $(foreach obj, $(CPPS:.cpp=.o), $(OBJ_DIR)/$(obj))
dir_guard=@mkdir -p $(@D)

all: x.macrobench.$(WORKLOAD)

x.macrobench.$(WORKLOAD): $(OBJS)
	$(CC) -o $@ $^ $(LDFLAGS)

$(OBJ_DIR)/%.o: %.cpp
	$(dir_guard)
	$(CC) -c $(CFLAGS) -o $@ $<

clean:
	@rm -f x.macrobench.*
	@rm -rf $(OBJ_DIR)
