import re
import matplotlib.pyplot as plt
import os


CHECKPOINT_INTERVAL = 150
FAILURE_STEP = 280

MPI_LABEL = 'OpenMPI+checkpoint'
WRITEFIRST_LABEL = 'WriteFirst'
LINEAGE_STASH_LABEL = 'Lineage stash'

def parse_mpi(directory):
    mpi_times = []
    for filename in os.listdir(directory):
        if 'failure-mpi' not in filename:
            continue
        with open(os.path.join(directory, filename), 'r') as f:
            for line in f.readlines():
                fields = line.split(',')
                if len(fields) == 3:
                    try:
                        step = int(fields[0])
                    except:
                        continue
                    current_time = float(fields[2]) / 1e3
                    if step != len(mpi_times):
                        continue
                    mpi_times.append(current_time)

    print(len(mpi_times))
    mpi_latencies = []
    t = mpi_times.pop(0)
    for t2 in mpi_times:
        mpi_latencies.append(t2 - t)
        t = t2
    print("MPI recovery time:", max(mpi_latencies))
    return mpi_latencies

def parse_lineage_stash(directory):
    writefirst_latencies = []
    lineage_stash_latencies = []
    for filename in os.listdir(directory):
        if not filename.startswith('failure-latency'):
            continue
        if '0-gcs-' in filename:
            latencies = lineage_stash_latencies
        else:
            latencies = writefirst_latencies

        with open(os.path.join(directory, filename), 'r') as f:
            for line in f.readlines():
                if "Finished" in line:
                    latency = line.split(' ')[-1]
                    latencies.append(float(latency))
    print("WriteFirst recovery time:", max(writefirst_latencies))
    print("Lineage stash recovery time:", max(lineage_stash_latencies))
    return writefirst_latencies, lineage_stash_latencies



def plot(latencies, save_filename, lineage_stash_offset):
    fig, ax = plt.subplots()

    START = 275
    END = 299
    SAVE = False

    # The Ray runs don't kill at exactly round 280.
    OFFSETS = {
        MPI_LABEL: 0,
        LINEAGE_STASH_LABEL: lineage_stash_offset,
        WRITEFIRST_LABEL: lineage_stash_offset,
    }

    for label, points in latencies:
        plt.plot(range(START, END), points[START+OFFSETS[label]:END+OFFSETS[label]], label=label, linewidth=2)
    ax.set_yscale('log')
    #ax.set_ylim(0.3)

    ax.set_ylabel("Iteration time (s)")
    ax.set_xlabel("Iteration")
    plt.rc('font', size=6.5)
    plt.legend()
    plt.tight_layout()

    if save_filename is not None:
        plt.savefig("recovery-{}".format(save_filename))
    else:
        plt.show()

def main(directory, save_filename, lineage_stash_offset):
    latencies = []
    latencies.append((MPI_LABEL, parse_mpi(directory)))
    writefirst_latencies, lineage_stash_latencies = parse_lineage_stash(directory)
    latencies.append((WRITEFIRST_LABEL, writefirst_latencies))
    latencies.append((LINEAGE_STASH_LABEL, lineage_stash_latencies))
    plot(latencies, save_filename, lineage_stash_offset)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Benchmarks.')
    parser.add_argument(
            '--directory',
            type=str,
            default='.')
    parser.add_argument(
            '--lineage-stash-offset',
            type=int,
            default=0,
            help="How much to offset the lineage stash plots by, since the failure time is not exact.")
    parser.add_argument(
            '--save-filename',
            type=str,
            default=None)
    args = parser.parse_args()

    main(args.directory, args.save_filename, args.lineage_stash_offset)
