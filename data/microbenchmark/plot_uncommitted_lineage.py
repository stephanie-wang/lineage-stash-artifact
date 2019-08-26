import csv
import re
import os
from collections import defaultdict
from collections import namedtuple
import matplotlib.pyplot as plt
import numpy as np

FIELDS = [
    'workers',
    'shards',
    'gcs',
    'gcsdelay',
    'nondeterminism',
    'task',
    'failures',
]

Label = namedtuple('Label', FIELDS)

def parse_lineage(directory):
    regex = 'lineage-'
    for field in FIELDS:
        regex +='(?P<{field}>.*)-{field}-'.format(field=field)

    results = {}
    num_nodes = None
    for filename in os.listdir(directory):
        g = re.match(regex, filename)
        if g is None:
            continue
        fields = g.groupdict()
        for field, val in fields.items():
            fields[field] = int(val)
        label = Label(**fields)
        lineages = defaultdict(lambda: defaultdict(int))
        with open(os.path.join(directory, filename), 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                worker = row['worker']
                num_tasks, size = int(row['num_tasks']), int(row['uncommitted_lineage'])
                lineages[worker][num_tasks] = size

        aggregate_sizes = defaultdict(int)
        for worker, lineage_sizes in lineages.items():
            for num_tasks, size in lineage_sizes.items():
                aggregate_sizes[num_tasks] += size

        sizes, weights = zip(*aggregate_sizes.items())
        lineage_size_mean = np.average(sizes, weights=weights)
        lineage_size_std = np.sqrt(np.average((sizes - lineage_size_mean) ** 2, weights=weights))
        results[label] = (lineage_size_mean, lineage_size_std)

        if num_nodes is None:
            num_nodes = label.workers
        assert num_nodes == label.workers

    return results, num_nodes

def plot(rows, save_filename):
    fig, ax = plt.subplots(figsize=(6, 3.25))

    for label, row in rows:
        x, ys = zip(*row)
        y = [point[0] for point in ys]
        errs = [point[1] for point in ys]
        plt.errorbar(x, y, yerr=errs, capsize=3, linewidth=2, label="$f$={}".format(label))

    plt.ylabel('Uncommitted\nlineage size')
    plt.xlabel('Task duration (ms)')
    plt.legend()
    font = {'size': 18}
    plt.rc('font', **font)
    plt.tight_layout()

    if save_filename is not None:
        plt.savefig(save_filename)
    else:
        plt.show()

def save_csv(csv_filename, rows):
    rows.sort(key=lambda row: row[0])
    fields = ['f', 'task_duration_ms', 'uncommitted_lineage']

    with open(csv_filename, 'w+') as f:
        w = csv.DictWriter(f, fields)
        w.writeheader()
        for label, row in rows:
            x, ys = zip(*row)
            y = [point[0] for point in ys]
            for i, j in zip(x, y):
                w.writerow({
                    'f': label,
                    'task_duration_ms': i,
                    'uncommitted_lineage': j,
                })


def main(directory, save_filename):
    results, num_nodes = parse_lineage(directory)


    x_field = 'task'
    row_field = 'failures'

    rows = defaultdict(list)
    for label, value in results.items():
        assert label.gcs == 0 and label.nondeterminism == 1
        key = getattr(label, row_field)
        if key == -1:
            key = num_nodes
        row_label = key
        rows[row_label].append((getattr(label, x_field), value))
    for _, row in rows.items():
        row.sort(key=lambda item: item[0])
    rows = list(rows.items())
    rows.sort(key=lambda row: row[0])

    plot(rows, save_filename)

    if save_filename is not None:
        csv_filename = '.'.join(save_filename.split('.')[:-1])
        csv_filename += '.csv'
        save_csv(csv_filename, rows)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Benchmarks.')
    parser.add_argument(
        '--directory',
        default='data/19-04-11-15-58-01',
        help="Relative path to the directory with data files. Should be in format 'latency-<date>'"
        )
    parser.add_argument(
            '--save-filename',
            type=str,
            default=None)
    args = parser.parse_args()
    main(args.directory, args.save_filename)
