import os
DIRECTORY = 'data'
import csv
import numpy as np
import matplotlib.pyplot as plt
from collections import defaultdict


def parse_latencies(filename, flink):
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

            if operator is None:
                operator = row['sink_id']
                first_timestamp = float(row['timestamp'])
                max_timestamp = record_timestamp


            if row['sink_id'] == operator:
                if flink:
                    # For Flink only, skip records that are older than what we have already seen,
                    # to ignore recovery stats
                    # Not necessary for lineage stash since we should never receive duplicate records.
                    if record_timestamp > max_timestamp:
                        max_timestamp = record_timestamp
                    else:
                        continue

                latency = float(row['latency'])
                current_time = record_timestamp
                current_time -= first_timestamp
                if in_seconds:
                    latency *= 1000
                else:
                    current_time /= 1000
                current_time = int(current_time)
                points.append((current_time, latency))
            else:
                break
    points.sort(key=lambda item: item[0])
    return points

def parse_throughputs(filename):
    operator = None
    first_timestamp = None
    max_timestamp = None

    with open(filename, 'r') as f:
        reader = csv.DictReader(f)
        operator_throughputs = defaultdict(lambda: defaultdict(list))
        for row in reader:
            timestamp = row['cur_time']
            in_seconds = '.' in timestamp
            timestamp = float(timestamp)

            if operator is None or operator != row['sink_id']:
                operator = row['sink_id']
                first_timestamp = timestamp
                max_timestamp = float(row['timestamp'])

            if row['sink_id'] == operator:
                record_timestamp = float(row['timestamp'])
                # Skip records that are older than what we have already seen, to ignore recovery stats.
                if record_timestamp > max_timestamp:
                    max_timestamp = record_timestamp
                else:
                    continue

                # Floor the timestamp for the throughput measurement.
                timestamp = (timestamp - first_timestamp)
                if not in_seconds:
                    timestamp /= 1000
                timestamp = int(timestamp)
                throughput = float(row['throughput'])
                operator_throughputs[operator][timestamp].append(throughput)
    throughputs = defaultdict(int)
    for operator_id, operator_throughput in operator_throughputs.items():
        operator_throughput = dict((timestamp, np.mean(tputs)) for timestamp, tputs in operator_throughput.items())
        # Find the missing timestamps for this operator.
        min_timestamp = min(operator_throughput)
        max_timestamp = max(operator_throughput)
        missing_timestamps = []
        for timestamp in range(min_timestamp, max_timestamp + 1):
            if timestamp not in operator_throughput:
                missing_timestamps.append(timestamp)
        # Fill out the missing throughputs.
        for timestamp in missing_timestamps:
            # If either the timestamp before or after is also missing, then this operator was down.
            # Otherwise, we just missed a throughput measurement when logging.
            if timestamp - 1 in missing_timestamps or timestamp + 1 in missing_timestamps:
                operator_throughput[timestamp] = 0
            else:
                operator_throughput[timestamp] = np.mean((operator_throughput[timestamp - 1],
                                                        operator_throughput[timestamp + 1]))

        for timestamp, tput in operator_throughput.items():
            throughputs[timestamp] += np.mean(tput)


    throughputs = list(throughputs.items())
    throughputs.sort(key=lambda item: item[0])
    return throughputs

def plot_latencies(rows, save_filename):
    fig, ax = plt.subplots()
    for label, row, _ in rows:
        x, y = zip(*row)
        plt.plot(x, y, label=label, linewidth=2)
    
    plt.ylabel('Latency (ms)')
    plt.xlabel('Time since start (s)')
    plt.legend()
    font = {'size': 24}
    plt.rc('font', **font)
    plt.tight_layout()
    plt.yscale('log')
    
    if save_filename is not None:
        plt.savefig('latency-{}'.format(save_filename))
    else:
        plt.show()

def plot_throughputs(rows, save_filename):
    fig, ax = plt.subplots()
    for label, _, row in rows:
        x, y = zip(*row)
        y = [point / 100000 for point in y]
        plt.plot(x, y, label=label, linewidth=2)
    
    plt.ylabel('Throughput \n(100k records/s)')
    plt.xlabel('Time since start (s)')
    plt.legend()
    font = {'size': 24}
    plt.rc('font', **font)
    plt.tight_layout()
    
    if save_filename is not None:
        plt.savefig('throughput-{}'.format(save_filename))
    else:
        plt.show()


def mean_failure_latency(latencies):
    failure_time = 45
    recovery_time = 64
    failure_latencies = []
    for timestamp, latency in latencies:
        if timestamp > failure_time and timestamp < recovery_time:
            failure_latencies.append(latency)
    return np.mean(failure_latencies)

def main(directory, save_filename, global_downsample):
    flink_filename = None
    lineage_stash_filename = None
    writefirst_filename = None
    for filename in os.listdir(directory):
        if filename.startswith('failure-flink-latency'):
            if flink_filename is not None:
                print("WARNING: multiple Flink filenames found, skipping {}".format(flink_filename))
            flink_filename = os.path.join(directory, filename)
        elif filename.startswith('failure-latency'):
            if lineage_stash_filename is None:
                print("WARNING: multiple lineage stash filenames found, skipping {}".format(lineage_stash_filename))
            lineage_stash_filename = os.path.join(directory, filename)
        elif filename.startswith('writefirst-failure-latency'):
            if writefirst_filename is None:
                print("WARNING: multiple WriteFirst filenames found, skipping {}".format(writefirst_filename))
            writefirst_filename = os.path.join(directory, filename)

    flink_throughput_filename = flink_filename.replace('latency', 'throughput')
    lineage_stash_throughput_filename = lineage_stash_filename.replace('latency', 'throughput')
    writefirst_throughput_filename = writefirst_filename.replace('latency', 'throughput')

    FILENAMES = [
        ('Flink',
        flink_filename,
        flink_throughput_filename,
        True),
        ('WriteFirst',
        writefirst_filename,
        writefirst_throughput_filename,
        False),
        ('Lineage stash',
        lineage_stash_filename,
        lineage_stash_throughput_filename,
        False),
    ]
    stats = []
    for label, latency_filename, throughput_filename, is_flink in FILENAMES:
        latencies = parse_latencies(latency_filename, is_flink)
        print(label, len(latencies), "latency samples")
        throughputs = parse_throughputs(throughput_filename)
        stats.append((label, latencies, throughputs))

    # Downsample so that all jobs have the same number of latency samples.
    min_latency_samples = min(len(latencies) for _, latencies, _ in stats)
    for i, stat in enumerate(stats):
        label, latencies, throughputs = stat
        downsample_factor = len(latencies) // min_latency_samples * global_downsample
        latencies = [latencies[i] for i in range(0, len(latencies), downsample_factor)]
        stats[i] = (label, latencies, throughputs)
        print("Downsampled", label, "by", downsample_factor, "to", len(latencies), "records")

    plot_latencies(stats, save_filename)
    plot_throughputs(stats, save_filename)

    for label, latency, _ in stats:
        print(label, "mean latency during recovery:", mean_failure_latency(latency))


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Benchmarks.')
    parser.add_argument(
            '--directory',
            type=str,
            default='32-workers')
    parser.add_argument(
            '--downsample',
            type=int,
            default=10,
            help="The amount to downsample all latency samples by.")
    parser.add_argument(
            '--save-filename',
            type=str,
            default=None)
    args = parser.parse_args()

    main(args.directory, args.save_filename, args.downsample)
