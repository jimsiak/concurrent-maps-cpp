#!/bin/bash

echo "Estimated running time of the experiment is 2 hours."
echo

#THREAD_COUNTS="24 48 96"
THREAD_COUNTS="1 2 4 8 16 22 44"

if [ "$#" -eq 1 ]; then
    THREAD_COUNTS=$1
fi

rm log.txt results.csv 2>/dev/null

printf "%30s %20s %20s %20s %20s\n" alg nthreads theta runtime throughput | tee -a results.csv

for z in 0.01 0.1 0.5 0.9 ; do
    for n in $THREAD_COUNTS; do
        args="-t$n -s`echo "1024*1024*96" | bc` -r0.9 -w0.1 -z$z -pin 0-$((n-1))"
        for alg in `cd ../../bin ; ls rundb_YCSB*` ; do
            f=temp.$alg.txt
            bash -c "cd ../.. ; ASAN_OPTIONS=new_delete_type_mismatch=0 LD_PRELOAD=../lib/libjemalloc.so ./bin/$alg $args" 2>&1 > $f
	    exitcode="$?"
	    if [ "$exitcode" -ne 0 ]; then
                echo "Experiment exited with status code $exitcode, see the log file: $f"
		echo "------"
		cat "$f"
		echo "------"
		exit $exitcode
            fi
            cat $f >> log.txt
            sum=`grep "summary" $f`
            tput=`echo $sum | cut -d" " -f32 | tr -d "a-zA-Z=,_"`
            runtime=`echo $sum | cut -d" " -f4 | tr -d "a-zA-Z=,_"`
            shortalg=`echo $alg | cut -d"_" -f3-`
            printf "%30s %20s %20s %20s %20s\n" "$shortalg" "$n" "$z" "$runtime" "$tput" | tee -a results.csv
        done
    done
done

echo
echo "Finished: see results.csv"
