import pickle
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


def output_stats(in_filename, float_size, for_paper=True):
    print("")
    data = pd.read_csv(in_filename)

    mpi_data = data[data["num_float32"] == float_size].sort_values("num_nodes")

    for num_nodes in pd.unique(mpi_data["num_nodes"]):
        values = mpi_data[mpi_data["num_nodes"] == num_nodes]["millis"].values
        mean = values.mean().round()
        std = values.std().round()
        if for_paper:
            print("({},{}),".format(mean, std))
        else:
            # system,num_workers,size,num_iterations,mean,std
            print("mpi,{},{},{},{},{}".format(num_nodes, float_size*4, 10, mean, std))

output_stats("mpi-results-pernode.txt", 2500000, False)
output_stats("mpi-results-pernode.txt", 25000000, False)
output_stats("mpi-results-pernode.txt", 250000000, False)
