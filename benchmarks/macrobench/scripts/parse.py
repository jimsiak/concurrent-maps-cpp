#!/usr/bin/env python

import sys
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import yaml

def get_max_throughput_per_nthreads(bench, results):
	'''
	@bench: TPCC or YCSB
	@results: the list of dictionaries for 'bench'
	'''
	ret = dict()
	filtered_by_ntreads = filter_results("nthreads", results[bench])
	for nthreads in sorted(filtered_by_ntreads.keys()):
		results_sorted = sorted(filtered_by_ntreads[nthreads], key=lambda entry: float(entry['summary_stats']['throughput']), reverse=True)
		ret[nthreads] = results_sorted[0]
	return ret

def get_dict_with_nthreads(results):
	'''
	@results: { dict(), dict(), dict(), ... }
	@returns: { 1: dict(), 2: dict(), ... }
	'''
	ret = dict()
	for entry in results:
		nthreads = entry['nthreads']
		ret[nthreads] = entry
	return ret

def get_nthreads_and_throughput_arrays(results):
	'''
	@results: { 1: dict(), 2: dict(), ... }
	@returns: ( [1, 2, 4, 8, 16, 22, 44], [1.231, 2.311, ... ] )
	'''
	nthreads = []
	throughputs = []
	for t in sorted(results.keys()):
		nthreads.append(t)
		throughputs.append(float(results[t]['summary_stats']['throughput']))
	return (nthreads, throughputs)

def filter_results(field, results):
	"""
	"""
	filtered = dict()
	for entry in results:
		val = entry[field]
		if not val in filtered:
			filtered[val] = []
		filtered[val].append(entry)
	return filtered

def parse_output_file(filename):
	"""
	Reads a file that contains output from the benchmarks.
	It returns a list which contains one dictionary per run.
	"""
	ret = []
	fp = open(filename)
	line = fp.readline()
	while line:
		line = line.strip()
		tokens = line.split()

		if line.startswith("mem_allocator initialized!"):
			run_stats = dict()
			per_index_stats = dict()
		if line.startswith("Running "):
			run_stats['bench'] = tokens[1]
		elif line.startswith("Reading schema file:"):
			run_stats['schemafile'] = tokens[3]
		elif line.startswith("Initiating Map"):
			if "type:" in line:
				tokens = line.split('type: ')
				dsandsynctype = tokens[1][:-2]
				dsandsynctype = dsandsynctype.replace("(a,b)-", "ab")
				dsandsynctype = dsandsynctype.replace("(a-b)-", "ab")
				dstype = dsandsynctype.split('(')[0].strip()
				if not '(' in dsandsynctype:
					sttype = "None"
				else:
					sttype = dsandsynctype.split('(')[1].replace(')', '')
				run_stats['index-type'] = dstype + " (" + sttype + ")"
		elif line.startswith("[ type: "):
			tokens = line.split('type: ')
			dsandsynctype = tokens[1][:-2]
			dsandsynctype = dsandsynctype.replace("(a-b)-", "ab")
			dstype = dsandsynctype.split('(')[0].strip()
			if not '(' in dsandsynctype:
				sttype = "None"
			else:
				sttype = dsandsynctype.split('(')[1].replace(')', '')
			run_stats['index-type'] = dstype + " (" + sttype + ")"
		elif line.startswith("running "):
			run_stats['nthreads'] = int(tokens[1])
		elif line.startswith("Per-index stats"):
			line = line.replace("Per-index stats: ", '')
			stats = summary_to_dict(line)
			index_name = stats['index']
			per_index_stats[index_name] = stats
		elif line.startswith("[summary]"):
			line = line.replace('[summary] ', '')
			stats = summary_to_dict(line)
			run_stats['per_index_stats'] = per_index_stats
			run_stats['summary_stats'] = stats
			ret.append(run_stats)
			
		line = fp.readline()

	return ret

def create_plot(sttypes, nthreads_arrays, numbers_arrays, plot_title):
	plt.figure()
	ax = plt.subplot("111")

	for i, sttype in enumerate(sttypes):
		nthreads = nthreads_arrays[i]
		throughputs = numbers_arrays[i]
		ax.plot(np.arange(len(nthreads)), throughputs, marker='x', linewidth=2, markersize=12, markeredgewidth=2, label=sttype)

	leg = ax.legend(ncol=1,loc="best",prop={'size':14})
	plt.title(plot_title)
	plt.savefig(plot_title.replace("/", "-") + ".png", bbox_inches = 'tight')

def summary_to_dict(line):
	ret = dict()
	tokens = line.split(',')
	for token in tokens:
		newtokens = token.split('=')
		ret[newtokens[0].strip()] = newtokens[1]
	return ret

for infile in sys.argv[1:]:
	all_results = []
	all_results += parse_output_file(infile)

#	print yaml.dump(ret)
filtered = filter_results("bench", all_results)
for bench in sorted(filtered.keys()):

	labels = []
	nt = []
	th = []

	best = get_max_throughput_per_nthreads(bench, filtered)
	ret = get_nthreads_and_throughput_arrays(best)
	labels.append("best")
	nt.append(ret[0])
	th.append(ret[1])

	per_ds_results = filter_results("index-type", filtered[bench])
	print per_ds_results.keys()

	for ds in per_ds_results.keys(): 
		if not "RCU-HTM" in ds:
			continue
		ds_results = get_dict_with_nthreads(per_ds_results[ds])
		ret = get_nthreads_and_throughput_arrays(ds_results)
		labels.append(ds)
		nt.append(ret[0])
		th.append(ret[1])

	create_plot(labels, nt, th, "best-"+bench)



	per_thread_filtered = filter_results("nthreads", filtered[bench])
	for t in sorted(per_thread_filtered.keys()):
		results_sorted = sorted(per_thread_filtered[t], key=lambda entry: float(entry['summary_stats']['throughput']), reverse=True)
#		if bench == "YCSB":
#			results_sorted = sorted(per_thread_filtered[t], key=lambda entry: float(entry['per_index_stats']['MAIN_INDEX']['throughput']), reverse=True)
#		else:
#			results_sorted = sorted(per_thread_filtered[t], key=lambda entry: float(entry['per_index_stats']['DISTRICT_IDX']['throughput']), reverse=True)
			
		print "Bench " + bench + " threads " + str(t)

		for i, r in enumerate(results_sorted):
			print "    " + str(i) + ": " + r["index-type"] + " " + r["summary_stats"]["throughput"]
#			if bench == "YCSB":
#				print "    " + str(i) + ": " + r["index-type"] + " " + r['per_index_stats']['MAIN_INDEX']['throughput']
#			else:
#				print "    " + str(i) + ": " + r["index-type"] + " " + r['per_index_stats']['DISTRICT_IDX']['throughput']
