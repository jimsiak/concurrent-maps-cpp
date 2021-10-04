CFLAGS =
LDFLAGS =
CC = g++

CFLAGS += -DMEMORY_STATS=if\(1\) -DMEMORY_STATS2=if\(0\)
CFLAGS += -DMAX_THREADS_POW2=256
CFLAGS += -DCPU_FREQ_GHZ=2.1 #$(shell ../microbench/experiments/get_cpu_ghz.sh)
CFLAGS += -g -std=c++14 -O0
CFLAGS += -fopenmp
CFLAGS += $(xargs)
CFLAGS += -Wno-format
CFLAGS += -DNO_CLEANUP_AFTER_WORKLOAD
CFLAGS += -mrtm
CFLAGS += -Wno-write-strings

### if you do not have PAPI, comment out these two lines
#CFLAGS += -DUSE_PAPI
#LDFLAGS += -lpapi
#
#LDFLAGS += -lnuma

#workload=TPCC
workload=YCSB
data_structure_name=brown_ext_abtree_lf

.SUFFIXES: .o .cpp .h
bindir=bin
odir=OBJS
SRC_DIRS = ./ ./benchmarks/ ./concurrency_control/ ./storage/ ./storage/index/ ./system/ 
CFLAGS += -I../ $(patsubst %,-I%,$(SRC_DIRS))
CFLAGS += `find ../../../../trevor_brown_ppopp20_interpolation_trees/common -type d | sed s/^/-I/`
CFLAGS += -I../../lib/

CFLAGS += -DNOGRAPHITE=1 -DWORKLOAD=$(workload)
CFLAGS += -DALIGNED_ALLOCATIONS
CFLAGS += -fno-omit-frame-pointer
LDFLAGS += $(CFLAGS)
LDFLAGS += -L. -L../lib -g
LDFLAGS += -lpthread

CPPS = $(foreach dir, $(SRC_DIRS), $(wildcard $(dir)*.cpp))
OBJS = $(foreach obj, $(CPPS:.cpp=.o), $(odir)/$(obj))
dir_guard=@mkdir -p $(@D)

all: x.macrobench.$(workload)

x.macrobench.$(workload): $(OBJS)
	$(CC) -o $@ $^ $(LDFLAGS)

$(odir)/%.o: %.cpp
	$(dir_guard)
	$(CC) -c $(CFLAGS) -o $@ $<

clean:
	@rm -f x.macrobench.$(workload)
	@rm -rf $(odir)
