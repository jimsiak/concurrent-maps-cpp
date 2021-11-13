#!/usr/bin/env bash

export LD_PRELOAD=/home/users/jimsiak/concurentDataBenchmarks/trevor_brown_ppopp20_interpolation_trees/lib/libjemalloc.so
export LD_LIBRARY_PATH=/various/common_tools/gcc-5.3.0/lib64/
taskset -c 0 $BASHPID

###### DATA STRUCTURES ######
seqds=("treap" "abtree" "btree" "bst-unb-int" "bst-unb-ext" "bst-unb-pext" "bst-avl-int" "bst-avl-ext" "bst-avl-pext")
#############################

###### GLOBAL CONFIG ######
ITERATIONS=10
t=1 ## nthreads
st="" ## sync-type
###########################

for iteration in $(seq 0 $((ITERATIONS-1))); do
	OUTFILE=scripts/outputs/serial-$(git rev-parse HEAD).${iteration}.out

for bench in TPCC YCSB; do
	echo "========================> iter: $iteration bench: $bench <========================"

	for ds in ${seqds[@]}; do
		echo "----------------------> $t threads: $ds - $st <----------------------"
		./x.macrobench.$bench --data-structure=$ds --sync-type=$st -t$t &>> $OUTFILE
	done
	
done # bench
done # iteration
