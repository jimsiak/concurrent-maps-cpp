#!/usr/bin/env bash

export LD_PRELOAD=/home/users/jimsiak/concurentDataBenchmarks/trevor_brown_ppopp20_interpolation_trees/lib/libjemalloc.so
export LD_LIBRARY_PATH=/various/common_tools/gcc-5.3.0/lib64/
taskset -cp `seq -s, 0 21`,`seq -s, 44 65` $BASHPID

###### DATA STRUCTURES ######
seqds=("treap" "abtree" "btree" "bst-unb-int" "bst-unb-ext" "bst-unb-pext" "bst-avl-int" "bst-avl-ext" "bst-avl-pext")
synctypes=("cg-htm" "cg-rwlock" "cg-spinlock")

lockds=("bst-avl-bronson" "bst-avl-drachsler" "bst-avl-cf")
#lockfreeds=("bst-unb-natarajan" "bst-unb-ellen" "bst-unb-howley" "ist-brown" "abtree-brown" "abtree-brown-3path" "abtree-brown-llxscx" "bst-brown-3path" "bst-brown-llxscx" "bwtree-wang")
lockfreeds=("bst-unb-natarajan" "bst-unb-ellen" "ist-brown" "abtree-brown" "abtree-brown-3path" "abtree-brown-llxscx" "bst-brown-3path" "bst-brown-llxscx")
copds=("avl-int-cop" "avl-ext-cop")
nosynctypes=("NONE")

#rcuhtmds=("abtree" "btree" "bst-unb-int" "bst-unb-ext" "bst-unb-pext" "bst-avl-int" "bst-avl-ext" "bst-avl-pext")
rcuhtmds=("abtree" "btree" "bst-avl-int" "bst-avl-ext" "bst-avl-pext")
rcuhtmsynctypes=("rcu-htm" "rcu-sgl")

cads=("treap")
casynctypes=("ca-locks")
#############################

###### GLOBAL CONFIG ######
ITERATIONS=10
NTHREADS=(1 2 4 8 16 22 44)
###########################

for iteration in $(seq 0 $((ITERATIONS-1))); do
	OUTFILE=scripts/outputs/all-$(git rev-parse HEAD).${iteration}.out

for bench in TPCC YCSB; do
	echo "========================> iter: $iteration bench: $bench <========================"

for t in ${NTHREADS[@]}; do

	for st in ${synctypes[@]}; do
	for ds in ${seqds[@]}; do
		echo "----------------------> $t threads: $ds - $st <----------------------"
		./x.macrobench.$bench --data-structure=$ds --sync-type=$st -t$t &>> $OUTFILE
	done
	done

	for st in ${nosynctypes[@]}; do
	for ds in ${lockds[@]} ${lockfreeds[@]} ${copds[@]}; do
		echo "----------------------> $t threads: $ds - $st <----------------------"
		./x.macrobench.$bench --data-structure=$ds --sync-type=$st -t$t &>> $OUTFILE
	done
	done

	for st in ${rcuhtmsynctypes[@]}; do
	for ds in ${rcuhtmds[@]}; do
		echo "----------------------> $t threads: $ds - $st <----------------------"
		./x.macrobench.$bench --data-structure=$ds --sync-type=$st -t$t &>> $OUTFILE
	done
	done

	for st in ${casynctypes[@]}; do
	for ds in ${cads[@]}; do
		echo "----------------------> $t threads: $ds - $st <----------------------"
		./x.macrobench.$bench --data-structure=$ds --sync-type=$st -t$t &>> $OUTFILE
	done
	done

done # t
done # bench
done # iteration
