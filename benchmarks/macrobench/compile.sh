#!/bin/bash
# 
# File:   compile.sh
# Author: trbot
#
# Created on May 28, 2017, 9:56:43 PM
#

workloads="YCSB TPCC"

## format is the following is
## data_structure_name:compilation_arguments
algs=( \
    "brown_ext_ist_lf:" \
    "brown_ext_abtree_lf:" \
	"siakavaras_ext_bptree_rcuhtm:-mrtm" \
)

make_workload_dict() {
    # compile the given workload and algorithm
    workload=$1
    name=`echo $2 | cut -d":" -f1`
    opts=`echo $2 | cut -d":" -f2-`
    opts_clean=`echo $opts | tr " " "." | tr "=" "-"`
    fname=log.compile.temp.$workload.$name.${opts_clean}.out
    #echo "arg1=$1 arg2=$2 workload=$workload name=$name opts=$opts"
    make -j clean workload="$workload" data_structure_name="$name" data_structure_opts="$opts"
    make -j workload="$workload" data_structure_name="$name" data_structure_opts="$opts" > $fname 2>&1
    if [ $? -ne 0 ]; then
        echo "Compilation FAILED for $workload $name $opts"
        mv $fname log.compile.failure.$workload.$name.${opts_clean}.txt
    else
        echo "Compiled $workload $name $opts"
        mv $fname log.compile.success.$workload.$name.${opts_clean}.txt
    fi
}
export -f make_workload_dict

rm -f log.compile.*.txt

## check for gnu parallel
#command -v parallel > /dev/null 2>&1
#if [ "$?" -eq "0" ]; then
#	parallel make_workload_dict ::: $workloads ::: "${algs[@]}"
#else
	for workload in $workloads; do
	for alg in "${algs[@]}"; do
		make_workload_dict "$workload" "$alg"
	done
	done
#fi

errorfiles=`ls log.compile.failure* 2> /dev/null`
numerrorfiles=`ls log.compile.failure* 2> /dev/null | wc -l`
if [ "$numerrorfiles" -ne "0" ]; then
    cat log.compile.failure*
    echo "ERROR: some compilation command(s) failed. See the following file(s)."
    for x in $errorfiles ; do echo $x ; done
else
    echo "Compilation successful."
fi
