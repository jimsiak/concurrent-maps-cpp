#!/usr/bin/env python

import sys, time
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib import cm
from mpl_toolkits.mplot3d import Axes3D
import numpy as np
import yaml

import pandas as pd
import seaborn as sns

def pandas_data(results):
	df = pd.DataFrame.from_dict(results)
	print df

#	pd.set_option('display.max_rows', None)
#	df = df.filter(['close_to_max', 'ds_full', 'num_threads', 'lookup_frac', 'init_tree_size', 'throughput'])
#	df = df[df['ds_full'].str.contains("rcu-htm")]
#	df = df.reset_index()
#	print df.sort_values(by=['close_to_max'])

#	df = df[df['num_threads'] == 1]
#	df = df[df['ds_full'].str.contains("cg-spinlock")]
#	df = df.filter(['close_to_max', 'ds_full', 'num_threads', 'lookup_frac', 'init_tree_size', 'throughput'])
#	df = df[df['close_to_max'] == 100.0]
#	df = df.reset_index()
#	print df

#	df = df[(df['ds_full'].str.contains("avl-bronson")) | (df['ds_full'].str.contains("avl-drachsler")) | (df['ds_full'].str.contains("avl-cf"))]
#	df = df[df['ds_full'].str.contains("btree")]

#	ax = sns.stripplot(x='lookup_frac', y='close_to_max', hue='num_threads', data=df, jitter=0.2)
#	ax = sns.stripplot(x='init_tree_size', y='close_to_max', hue='num_threads', data=df, jitter=0.2)
#	ax.set_ylim(0,110)
#	plt.xticks(rotation=90)

#	fig = plt.figure()
#	ax = Axes3D(fig)
#	ax.plot_trisurf(df['init_tree_size'], df['num_threads'], df['close_to_max'], cmap=cm.jet, linewidth=0.2)

#	title = "seaborn"
#	plt.savefig(title.replace("/", "-") + ".png", bbox_inches = 'tight')

def create_boxplot_figure(xlabels, boxplots, title):
	plt.figure()
	ax = plt.subplot("111")
	ax.boxplot(boxplots) #, whis=(0,100))
	ax.set_xticks(range(1, len(xlabels)+1))
	ax.set_xticklabels(xlabels, rotation=90)
	ax.set_ylim(0,100)
	plt.title(title)
	plt.savefig(title.replace("/", "-") + ".png", bbox_inches = 'tight')

def plot_close_to_max_per_nthreads(results, ds_substr):
	per_nt_close_to_max = dict()
	for entry in results:
		ds_full = entry['ds_full']
		nthreads = entry['num_threads']
		if not nthreads in per_nt_close_to_max:
			per_nt_close_to_max[nthreads] = []
		if ds_substr in ds_full:
			per_nt_close_to_max[nthreads].append(entry['close_to_max'])

	nt_names = sorted(per_nt_close_to_max.keys())
	boxplots = []
	for nt in nt_names:
		boxplots.append(per_nt_close_to_max[nt])

	create_boxplot_figure(nt_names, boxplots, "close_to_max_per_nthreads")


def plot_close_to_max_per_workload(results, ds_substr):
	per_wl_close_to_max = dict()
	for entry in results:
		ds_full = entry['ds_full']
		workload = entry['lookup_frac']
		if not workload in per_wl_close_to_max:
			per_wl_close_to_max[workload] = []
		if ds_substr in ds_full:
			per_wl_close_to_max[workload].append(entry['close_to_max'])

	for wl in per_wl_close_to_max.keys():
		print wl, len(per_wl_close_to_max[wl])

	wl_names = sorted(per_wl_close_to_max.keys())
	boxplots = []
	for wl in wl_names:
		boxplots.append(per_wl_close_to_max[wl])

	create_boxplot_figure(wl_names, boxplots, "close_to_max_per_workload")


def plot_close_to_max(results):
	per_ds_close_to_max = dict()
	for entry in results:
		ds_full = entry['ds_full']
		if not ds_full in per_ds_close_to_max:
			per_ds_close_to_max[ds_full] = []
		per_ds_close_to_max[ds_full].append(entry['close_to_max'])

	for ds in per_ds_close_to_max.keys():
		print ds, len(per_ds_close_to_max[ds])

	ds_names = per_ds_close_to_max.keys()
	ds_names = filter(lambda entry: 'cg-htm' in entry or 'rcu-htm' in entry, ds_names)
	boxplots = []
	for ds in ds_names:
		boxplots.append(per_ds_close_to_max[ds])

	create_boxplot_figure(ds_names, boxplots, "close_to_max")

def get_close_to_max_per_point(results):
	'''
	@ret: { nthreads: {lookup_frac: {init_tree_size: [dict(), dict(), ...] } } }
	'''
	ret = dict()
	for r in results:
		nthreads = r['num_threads']
		lookup_frac = r['lookup_frac']
		init_tree_size = r['init_tree_size']
		if not nthreads in ret:
			ret[nthreads] = dict()
		if not lookup_frac in ret[nthreads]:
			ret[nthreads][lookup_frac] = dict()
		if not init_tree_size in ret[nthreads][lookup_frac]:
			ret[nthreads][lookup_frac][init_tree_size] = []
		ret[nthreads][lookup_frac][init_tree_size].append(r)

	for nt in ret.keys():
		for lf in ret[nt].keys():
			for it in ret[nt][lf].keys():
				print nt, lf, it, len(ret[nt][lf][it])
				ret[nt][lf][it] = sorted(ret[nt][lf][it], key=lambda entry: entry['throughput'], reverse=True)
				cur_array = ret[nt][lf][it]
				cur_max_throughput = cur_array[0]['throughput']
				for entry in cur_array:
					entry['close_to_max'] = entry['throughput'] / cur_max_throughput * 100
	return ret
		

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

		if line.startswith("Inputs:"):
			run_stats = dict()

			## Read all input parameters
			line = fp.readline()
			line = fp.readline().strip()
			while len(line) > 0:
				tokens = line.split(':')
				run_stats[tokens[0].strip()] = tokens[1].strip()
				line = fp.readline().strip()
			if 'ds_name' in run_stats:
				run_stats['ds_full'] = run_stats['ds_name'] + "(" + run_stats['sync_type'] + ")"
		## This is here to make it work with the C runs in rbt-new folder
		elif line.startswith("RBT implementation"): 
			run_stats['ds_full'] = tokens[2]
		elif line.startswith("Throughput(Ops/usec):"):
			run_stats['throughput'] = float(tokens[1])
		elif line.startswith("Expected size of MAP") or \
		     line.startswith("Expected size of RBT"):
			run_stats['num_threads'] = int(run_stats['num_threads'])
			run_stats['lookup_frac'] = int(run_stats['lookup_frac'])
			run_stats['expected_final_size'] = int(tokens[4])
			ret.append(run_stats)
			
		line = fp.readline()

	return ret

## parse all input files
all_results = []
for infile in sys.argv[1:]:
	all_results += parse_output_file(infile)

total_results = len(all_results)
print "Total:", total_results
print yaml.dump(all_results)

per_ds_results = filter_results('ds_full', all_results)
for ds in per_ds_results.keys():
	print ds, len(per_ds_results[ds])

per_nthreads_results = filter_results('num_threads', all_results)
for nthreads in sorted(per_nthreads_results.keys()):
	print nthreads, len(per_nthreads_results[nthreads])

#all_results = filter(lambda entry: 'btree' in entry['ds_full'], all_results)

# Adds the 'close_to_max' attribute to every result
get_close_to_max_per_point(all_results)

#plot_close_to_max(all_results)
#plot_close_to_max_per_workload(all_results, "cg-spinlock")
#plot_close_to_max_per_nthreads(all_results, "cg-spinlock")

pandas_data(all_results)
