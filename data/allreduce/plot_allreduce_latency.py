import matplotlib

NUM_ROUNDS = 20


import os
import re
from collections import namedtuple
import numpy as np
import matplotlib.pyplot as plt
import copy
import csv

FIELDS = [
    'workers',
    'shards',
    'gcs',
    'gcsdelay',
    'bytes',
]
Label = namedtuple('Label', FIELDS)

# Dummy GCS value so we can identify MPI values.
MPI_GCS = 2
GCS_LABELS = ['WriteFirst', 'WriteFirst +1ms', 'Lineage stash']

def filter_field(rows, field, value):
    if value is None:
        return rows
    return [(row, v) for row, v in rows if getattr(row, field) == value]

def label_to_str(label, field):
    v = getattr(label, field)
    if field == "gcs":
        if v == 1:
            return "WriteFirst"
        elif v == 0:
            return "Lineage stash"
        else:
            return "OpenMPI"
    if field == "gcsdelay":
        if getattr(label, "gcs") == MPI_GCS:
            return ""
        else:
            return "+{}ms".format(v)
    return "{} {}".format(v, field)

def plot(rows, workers, shards, gcs, bytes, group_by_field, sort_by_fields, reverse=False, save_filename=None):
    fig, ax = plt.subplots(figsize=(3, 3.5))

    rows = filter_field(rows, "workers", workers)
    rows = filter_field(rows, "shards", shards)
    rows = filter_field(rows, "gcs", gcs)
    rows = filter_field(rows, "bytes", bytes)
    labels, values = zip(*rows)
    
    groups = set()
    for label in labels:
        groups.add(getattr(label, group_by_field))
    groups = sorted(list([int for int in groups]))

    all_sorts = []
    for sort_by_field in sort_by_fields:
        sorts = set()
        for label, value in rows:
            s = int(getattr(label, sort_by_field))
            sorts.add(s)
        sorts = sorted(list([int(s) for s in sorts]))
        if reverse:
            sorts.reverse()
        all_sorts.append(sorts)
    
    group_labels = []
    bars = []
    group_row = ""
    bar_row = [(0, 0) for _ in range(len(groups))]
    for sorts in reversed(all_sorts):
        group_row = [copy.deepcopy(group_row) for _ in sorts]
        bar_row = [copy.deepcopy(bar_row) for _ in sorts]
    group_labels = group_row
    bars = bar_row        

    num_bars = 0
    for label, value in rows:
        bargroup = bars
        label_subgroup = group_labels
        group_index = groups.index(int(getattr(label, group_by_field)))
        index = []
        for sorts, sort_by_field in zip(all_sorts, sort_by_fields):
            s = sorts.index(int(getattr(label, sort_by_field)))
            index.append(s)
        str_label = ""
        for i, s in enumerate(index):
            next_label = label_to_str(label, sort_by_fields[i])
            str_label += next_label

            if i == len(all_sorts) - 1:
                bargroup[s][group_index] = value
                label_subgroup[s] = str_label
                num_bars += 1
            else:
                bargroup = bargroup[s]
                label_subgroup = label_subgroup[s]
    num_bars /= len(bars)
    bar_width = 1.0 / (num_bars + 1)
    index = np.arange(len(groups))
    for i, bargroup in enumerate(bars): 
        for j, group in enumerate(bargroup):
            group, errs = zip(*group)
            x = index + (i * len(bargroup) + j) * bar_width - 0.5 + bar_width
            plt.bar(x, group, bar_width, yerr=errs, label=group_labels[i][j])
    ax.set_yscale('log')

    plt.xticks(index + bar_width, groups)
    #plt.legend(loc='lower right', framealpha=1)
    plt.legend()
    plt.xlabel("Array size (MB)")
    plt.ylabel("Duration (ms)")
#     plt.ylim(1, 5000)
    plt.xlim(-0.25)
    font = {'size'   : 10}
    plt.rc('font', **font)
    plt.tight_layout()

    if save_filename is not None:
        filename = "latency-{}".format(save_filename)
        plt.savefig(os.path.join('.', filename))
    else:
        plt.show()
    return rows

def parse_lineage_stash(directory, all_latencies):
    regex = 'latency-'
    for field in FIELDS:
        regex += '(?P<{field}>.*)-{field}-'.format(field=field)
    for filename in os.listdir(directory):
        g = re.match(regex, filename)
        if g is None:
            continue
        else:
            print(filename)
        fields = g.groupdict()
        fields['bytes'] = int(fields['bytes']) * 4 // 1e6
        for field, val in fields.items():
            fields[field] = int(val)
        label = Label(**fields)
        latencies = []
        with open(os.path.join(directory, filename), 'r') as f:
            for line in f.readlines():
                if "Finished" in line:
                    latency = line.split(' ')[-1]
                    latencies.append(float(latency))
        if len(latencies) > 0:
            latencies = latencies[-NUM_ROUNDS:]
            all_latencies[label] = latencies
            print(label, np.mean(latencies), np.std(latencies))

def parse_mpi(directory, all_latencies):
    MPI_FIELDS = [
        'workers',
        'bytes',
    ]

    regex = 'mpi-latency-'
    for field in MPI_FIELDS:
        regex += '(?P<{field}>.*)-{field}-'.format(field=field)

    for filename in os.listdir(directory):
        g = re.match(regex, filename)
        if g is None:
            continue
        else:
            print(filename)
        fields = g.groupdict()
        for field, val in fields.items():
            fields[field] = int(val)
        fields['bytes'] = int(fields['bytes']) * 4 // 1e6
        fields['gcs'] = MPI_GCS
        fields['shards'] = 1
        fields['gcsdelay'] = 0
        label = Label(**fields)
        latencies = []
        with open(os.path.join(directory, filename), 'r') as f:
            for line in f.readlines():
                fields = line.split(',')
                if len(fields) == 2 or len(fields) == 3:
                    try:
                        step = int(fields[0])
                    except:
                        continue
                    latencies.append(float(fields[1]) / 1e3)
        if len(latencies) > 0:
            latencies = latencies[-NUM_ROUNDS:]
            all_latencies[label] = latencies


def main(directory, save_filename):
    all_latencies = {}
    parse_lineage_stash(directory, all_latencies)
    parse_mpi(directory, all_latencies)

    means = [(label, (np.mean(values) * 1e3, np.std(values) * 1000)) for label, values in all_latencies.items()]
    print(means)

    plotted_rows = []
    num_workers = means[0][0].workers
    plotted_rows += plot(means,
        num_workers,
        None,
        None,
        None,
        'bytes',
        ['gcs', 'gcsdelay'],
        reverse=True,
        save_filename=save_filename)

    if save_filename is not None:
        # Write the plotted stats to an output CSV file.
        csv_filename = '.'.join(save_filename.split('.')[:-1] + ['csv'])
        order = ['workers', 'bytes', 'gcs', 'gcsdelay']
        # Stable sort.
        for field in reversed(order):
            plotted_rows.sort(key=lambda row: getattr(row[0], field))
        with open(csv_filename, 'w+') as f:
            fields = FIELDS + ['mean', 'stddev']
            w = csv.DictWriter(f, fields)
            w.writeheader()
            for label, value in plotted_rows:
                row = label._asdict()
                mean, stddev = value
                row['mean'] = mean
                row['stddev'] = stddev
                w.writerow(row)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Benchmarks.')
    parser.add_argument(
            '--directory',
            type=str,
            default='.')
    parser.add_argument(
            '--save-filename',
            type=str,
            default=None)
    args = parser.parse_args()

    main(args.directory, args.save_filename)
