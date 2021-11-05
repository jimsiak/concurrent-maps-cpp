#!/usr/bin/env bash

export LD_PRELOAD=/home/users/jimsiak/concurentDataBenchmarks/trevor_brown_ppopp20_interpolation_trees/lib/libjemalloc.so
export LD_LIBRARY_PATH=/various/common_tools/gcc-5.3.0/lib64/
export MT_CONF=`seq -s, 0 21`,`seq -s, 44 65`

###### DATA STRUCTURES ######
seqds=("treap" "abtree" "btree" "bst-unb-int" "bst-unb-ext" "bst-unb-pext" "bst-avl-int" "bst-avl-ext" "bst-avl-pext")
synctypes=("cg-htm" "cg-rwlock" "cg-spinlock")

lockds=("bst-avl-bronson" "bst-avl-drachsler" "bst-avl-cf" "bst-unb-ext-hohlocks")
lockfreeds=("bst-unb-natarajan" "bst-unb-ellen" "bst-unb-howley" "ist-brown" "abtree-brown" "abtree-brown-3path" "abtree-brown-llxscx" "bst-brown-3path" "bst-brown-llxscx" "bwtree-wang")
copds=("avl-int-cop" "avl-ext-cop")
nosynctypes=("NONE")

rcuhtmds=("abtree" "btree" "bst-unb-int" "bst-unb-ext" "bst-unb-pext" "bst-avl-int" "bst-avl-ext" "bst-avl-pext")
rcuhtmsynctypes=("rcu-htm" "rcu-sgl")

cads=("treap")
casynctypes=("ca-locks")
#############################

###### GLOBAL CONFIG ######
OUTFILE=scripts/outputs/seq-and-cg-$(git rev-parse HEAD).out
###########################

for bench in TPCC YCSB; do
	echo "========================> $bench <========================"

for t in 1 2 4 8 16 22; do
for st in ${synctypes[@]}; do
for ds in ${seqds[@]}; do
	echo "----------------------> $t threads: $ds - $st <----------------------"
	./x.macrobench.$bench --data-structure=$ds --sync-type=$st -t$t &>> $OUTFILE
done
done
done

done
