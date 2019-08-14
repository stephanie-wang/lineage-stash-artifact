import os
import numpy as np
import matplotlib.pyplot as plt
import csv

WARMUP_SECONDS = 30

def parse_latencies(filename):
    operator = None
    points = []
    first_timestamp = None
    max_timestamp = None
    with open(filename, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            record_timestamp = row['timestamp']
            in_seconds = '.' in record_timestamp
            record_timestamp = float(record_timestamp)

            if operator != row['sink_id']:
                operator = row['sink_id']
                first_timestamp = record_timestamp
                max_timestamp = record_timestamp

            if row['sink_id'] == operator:
                # Skip records during warmup.
                if in_seconds:
                    if record_timestamp - max_timestamp < WARMUP_SECONDS:
                        continue
                elif record_timestamp - max_timestamp < WARMUP_SECONDS * 1000:
                    continue

                latency = float(row['latency'])
                if in_seconds:
                    latency *= 1000
                points.append(latency)
    return points

def plot_latencies(all_latencies, save_filename):
    fig, ax = plt.subplots(figsize=(4, 2))
    
    for label, latencies in all_latencies:
        n, bin, patches = plt.hist(latencies, 1000, normed=True, cumulative=True, label=label,
                                   histtype='step', alpha=0.8, linewidth=2)
        patches[0].set_xy(patches[0].get_xy()[:-1])
    
    plt.ylabel('CDF')
    plt.xlabel('Latency (ms)')
    plt.legend(loc='lower right')
    font = {'size': 18}
    plt.rc('font', **font)
    plt.xlim(0,500)
    plt.ylim(0, 1)
    plt.tight_layout()
    
    if save_filename is not None:
        plt.savefig(save_filename)
    else:
        plt.show()

def main(directory, save_filename):
    flink_filename = None
    lineage_stash_filename = None
    writefirst_filename = None
    for filename in os.listdir(directory):
        if filename.startswith('flink-latency'):
            assert flink_filename is None
            flink_filename = os.path.join(directory, filename)
        elif filename.startswith('latency'):
            assert lineage_stash_filename is None
            lineage_stash_filename = os.path.join(directory, filename)
        elif filename.startswith('writefirst-latency'):
            assert writefirst_filename is None
            writefirst_filename = os.path.join(directory, filename)

    filenames = [
        ('Flink', flink_filename),
        ('WriteFirst', writefirst_filename),
        ('Lineage stash', lineage_stash_filename),
    ]
    all_latencies = []
    for label, filename in filenames:
        latencies = parse_latencies(filename)
        all_latencies.append((label, latencies))

    for label, latencies in all_latencies:
        print(label)
        print(np.min(latencies), np.max(latencies))
        print("mean={}, p0={}, p50={}, p90={}, p99={}, len={}".format(
            np.mean(latencies),
            np.percentile(latencies, 0),
            np.percentile(latencies, 50),
            np.percentile(latencies, 90),
            np.percentile(latencies, 99),
            len(latencies)))
    plot_latencies(all_latencies, save_filename)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Benchmarks.')
    parser.add_argument(
            '--directory',
            type=str,
            default='32-workers')
    parser.add_argument(
            '--save-filename',
            type=str,
            default=None)
    args = parser.parse_args()

    main(args.directory, args.save_filename)
