#!/usr/bin/env bash

git_commit=$(git rev-parse HEAD)
export LD_PRELOAD=/home/users/jimsiak/concurentDataBenchmarks/trevor_brown_ppopp20_interpolation_trees/lib/libjemalloc.so
export LD_LIBRARY_PATH=/various/common_tools/gcc-5.3.0/lib64/
export MT_CONF=`seq -s, 0 21`,`seq -s, 44 65`

###### DATA STRUCTURES ######
seqds=("treap" "abtree" "btree" "bst-unb-int" "bst-unb-ext" "bst-unb-pext" "bst-avl-int" "bst-avl-ext" "bst-avl-pext")
synctypes=("cg-htm" "cg-rwlock" "cg-spinlock")

lockds=("bst-avl-bronson" "bst-avl-drachsler" "bst-avl-cf" "bst-unb-citrus" "bst-unb-ext-hohlocks")
lockfreeds=("bst-unb-natarajan" "bst-unb-ellen" "bst-unb-howley" "ist-brown" "abtree-brown" "abtree-brown-3path" "abtree-brown-llxscx" "bst-brown-3path" "bst-brown-llxscx" "bwtree-wang")
copds=("avl-int-cop" "avl-ext-cop")
nosynctypes=("NONE")

rcuhtmds=("abtree" "btree" "bst-unb-int" "bst-unb-ext" "bst-unb-pext" "bst-avl-int" "bst-avl-ext" "bst-avl-pext")
rcuhtmsynctypes=("rcu-htm" "rcu-sgl")

cads=("treap")
casynctypes=("ca-locks")
#############################

###### GLOBAL CONFIG ######
TIMEOUT_DURATION=120
RUNTIME=5

workloads=(100_0_0 90_5_5 80_10_10 50_25_25 20_40_40 0_50_50)
treesizes=(100_100 1000_1K 10000_10K 1000000_1M)
threadnums=(1 2 4 8 16 22 44)

#OUTFILE=scripts/outputs/all-cstrkeys-noreclamation.out
OUTFILE=scripts/outputs/all-ullongkeys-noreclamation.${git_commit}.out

SEED1=$RANDOM
SEED2=$RANDOM
###########################

estimated_time=0
niterations=0
curiteration=0

run_microbench() {
	curiteration=$(($curiteration+1))
	sz=$1
	wl=$2
	t=$3
	ds=$4
	st=$5
	
	init_size=$(echo $sz | cut -d'_' -f'1')
	init_prefix=$(echo $sz | cut -d'_' -f'2')

	lookup_pct=$(echo $wl | cut -d'_' -f'1')
	insert_pct=$(echo $wl | cut -d'_' -f'2')
	delete_pct=$(echo $wl | cut -d'_' -f'3')

	echo "-----> [$curiteration / $niterations] $ds - $st : ($t threads, $init_prefix, $wl) <-----"
	timeout -sKILL $TIMEOUT_DURATION ./x.microbench -d $ds -f $st \
	               -s$init_size -m$((2*init_size)) \
	               -l$lookup_pct -i$insert_pct -t$t \
	               -e$SEED1 -j$SEED2 -r$RUNTIME &>> $OUTFILE
}

calculate_time() {
	nds=$((${#seqds[@]} * ${#synctypes[@]}))
	nlockds=$((${#lockds[@]} * ${#nosynctypes[@]}))
	nlfds=$((${#lockfreeds[@]} * ${#nosynctypes[@]}))
	ncopds=$((${#copds[@]} * ${#nosynctypes[@]}))
	nrcuhtmds=$((${#rcuhtmds[@]} * ${#rcuhtmsynctypes[@]}))
	ncads=$((${#cads[@]} * ${#casynctypes[@]}))
	ntotalds=$(($nds + $nlockds + $nlfds + $ncopds + $nrcuhtmds + $ncads))

	ntreesizes=${#treesizes[@]}
	nworkloads=${#workloads[@]}
	nthreadnums=${#threadnums[@]}

	total_rounds=$(($ntotalds * $ntreesizes * $nworkloads * $nthreadnums))

	total_time_sec=$(($total_rounds * $RUNTIME))
	estimated_time=$total_time_sec
	niterations=$total_rounds
}

calculate_time
echo "Estimated running time: $estimated_time sec ($(($estimated_time / 60)) minutes)"

SECONDS=0

for sz in ${treesizes[@]}; do
for wl in ${workloads[@]}; do
for t in ${threadnums[@]}; do

	## Coarse-grained data structures
	for st in ${synctypes[@]}; do
	for ds in ${seqds[@]}; do
		run_microbench $sz $wl $t $ds $st
	done
	done
	
	## Lock-based, lock-free and COP data structures
	for st in ${nosynctypes[@]}; do
	for ds in ${lockds[@]} ${lockfreeds[@]} ${copds[@]}; do
		run_microbench $sz $wl $t $ds $st
	done
	done
	
	## RCU-HTM and RCU-SGL data structures
	for st in ${rcuhtmsynctypes[@]}; do
	for ds in ${rcuhtmds[@]}; do
		run_microbench $sz $wl $t $ds $st
	done
	done
	
	## Contention-adaptive data structures
	for st in ${casynctypes[@]}; do
	for ds in ${cads[@]}; do
		run_microbench $sz $wl $t $ds $st
	done
	done

done
done
done

duration=$SECONDS
echo "Elapsed time: $duration sec ($(($duration / 60)) minutes)"
