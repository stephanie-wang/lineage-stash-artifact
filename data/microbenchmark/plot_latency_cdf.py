import re
import os
from collections import namedtuple
import numpy as np
import csv
import matplotlib.pyplot as plt


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

def label_to_str(label, num_nodes):
    system = ''
    if label.gcs == 0:
        system = 'Lineage stash'
    else:
        system = 'WriteFirst'
    if label.failures == -1 or label.gcs == 1:
        f = num_nodes
    else:
        f = label.failures
    if label.nondeterminism == 0:
        return "{}+{}ms".format(system, label.gcsdelay)
    else:
        return "{}+{}ms, $f$={}".format(system, label.gcsdelay, f)

def parse_latencies(directory):
    regex = 'latency-'
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
        latencies = []
        lineage_sizes = []
        lineage_size_weights = []
        with open(os.path.join(directory, filename), 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                latency = row['latency']
                latency = float(latency)
                latencies.append(latency)
        results[label] = np.array(latencies)
        print(label, np.mean(latencies))

        if num_nodes is None:
            num_nodes = label.workers
        assert num_nodes == label.workers
    return results, num_nodes

def plot_rows(fields, results, save_filename, num_nodes):
    rows = []
    for label, row in results.items():
        skip = False
        for filter_field in fields:
            val = getattr(label, filter_field[0])
            if val not in filter_field[1]:
                skip = True
                break
        if not skip:
            print(label)
            rows.append((label, row))

    rows.sort(key=lambda item: item[0].gcsdelay)
    rows.sort(key=lambda item: -1 * item[0].failures)
    rows.sort(key=lambda item: -1 * item[0].gcs)

    fig, ax = plt.subplots(figsize=(8, 4.5))
    lines = []
    labels = []
    for label, row in rows:
        n, bin, patches = plt.hist(row, 1000, normed=True, cumulative=True,
                 histtype='step', alpha=0.8, linewidth=3)
        patches[0].set_xy(patches[0].get_xy()[:-1])
        line, = plt.plot([], [], color=patches[0].get_edgecolor())
        lines.append(line)
        labels.append(label_to_str(label, num_nodes))


    plt.ylabel('CDF')
    plt.xlabel('Task latency (ms)')
    plt.legend(lines, labels, loc='lower right')
    font = {'size': 18}
    plt.rc('font', **font)
    plt.xlim(0, 25)
    plt.tight_layout()

    if save_filename is not None:
        print(fields)
        if 0 in dict(fields)['nondeterminism']:
            plt.savefig("deterministic-{}".format(save_filename))
        else:
            num_failures = dict(fields)['failures'][0]
            plt.savefig("nondeterministic-{}-failures-{}".format(num_failures, save_filename))
    else:
        plt.show()

    return rows

def save_csv(csv_filename, plotted_rows):
    order = ['nondeterminism', 'gcs', 'gcsdelay']

    # Stable sort.
    for field in reversed(order):
        plotted_rows.sort(key=lambda row: getattr(row[0], field))

    with open(csv_filename, 'w+') as f:
        fields = FIELDS + ['p50', 'p90', 'p95', 'p99']
        w = csv.DictWriter(f, fields)
        w.writeheader()
        for label, value in plotted_rows:
            row = label._asdict()
            for p in [50, 90, 95, 99]:
                row['p{}'.format(p)] = np.percentile(value, p)
            w.writerow(row)


def main(directory, save_filename):
    results, num_nodes = parse_latencies(directory)

    filter_fields = [
        [
        ('nondeterminism', [0]),
        ('gcsdelay', [0, 1, 5]),
        ('failures', [-1, 1]),
        ],
        [
        ('nondeterminism', [1]),
        ('gcsdelay', [0, 1, 5]),
        ('failures', [(num_nodes // 8), 1]),
        ],
        [
        ('nondeterminism', [1]),
        ('gcsdelay', [0, 1, 5]),
        ('failures', [-1, 1]),
        ],
    ]

    plotted_rows = []
    for fields in filter_fields:
        plotted_rows += plot_rows(fields, results, save_filename, num_nodes)

    if save_filename is not None:
        csv_filename = '.'.join(save_filename.split('.')[:-1])
        csv_filename += '.csv'
        save_csv(csv_filename, plotted_rows)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Benchmarks.')
    parser.add_argument(
        '--directory',
        default='latency-19-08-26-03-20-32',
        help="Relative path to the directory with data files. Should be in format 'latency-<date>'"
        )
    parser.add_argument(
            '--save-filename',
            type=str,
            default=None)
    args = parser.parse_args()
    main(args.directory, args.save_filename)
