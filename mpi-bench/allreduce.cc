#include <assert.h>
#include <math.h>
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include <chrono>
#include <iostream>
#include <fstream>
#include <cstring>

void print_mpi_stuff(){
  // Get the number of processes
  int world_size;
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);

  // Get the rank of the process
  int world_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

  // Get the name of the processor
  char processor_name[MPI_MAX_PROCESSOR_NAME];
  int name_len;
  MPI_Get_processor_name(processor_name, &name_len);

  // Print off a hello world message
  printf("Hello world from processor %s, rank %d out of %d processors\n",
	 processor_name, world_rank, world_size);
}

void noop(int *invec, int *inoutvec, int *len, MPI_Datatype *dtype){
  // Do nothing.
  return;
}

// Creates an array of random numbers. Each number has a value from 0 - 1
float *create_rand_nums(int num_elements) {
  float *rand_nums = (float *)malloc(sizeof(float) * num_elements);
  assert(rand_nums != NULL);
  int i;
  for (i = 0; i < num_elements; i++) {
    rand_nums[i] = (rand() / (float)RAND_MAX);
  }
  return rand_nums;
}

std::string GetCheckpointName(int64_t checkpoint_index) {
  int world_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
  std::string name = "/tmp/mpi-checkpoint-" + std::to_string(world_rank) + "-" + std::to_string(checkpoint_index);
  return name;
}

bool CheckpointExists(int64_t checkpoint_index) {
  std::string name = GetCheckpointName(checkpoint_index);
  std::ifstream f(name.c_str());
  return f.good();
}

void LoadCheckpoint(float *destination, size_t num_elements, int64_t checkpoint_index) {
  std::ifstream infile; 
  std::string name = GetCheckpointName(checkpoint_index);
  infile.open(name, std::ios::in);
  if (!infile.good()) {
    std::cout << "ERROR opening checkpoint " << checkpoint_index << " err:" << strerror(errno) << std::endl;
    exit(1);
  }

  std::cout << "Loading checkpoint " << name << std::endl;

  char *serialized = reinterpret_cast<char*>(destination); 
  infile.read(serialized, sizeof(float) * num_elements);
  if (infile.fail()) {
    std::cout << "ERROR loading checkpoint " << name << " err:" << strerror(errno) << std::endl;
    exit(1);
  }

  std::cout << "Done checkpoint " << checkpoint_index << std::endl;

  infile.close();
}

void SaveCheckpoint(const float *destination, size_t num_elements, int64_t checkpoint_index) {
  std::ofstream outfile; 
  std::string name = GetCheckpointName(checkpoint_index);
  outfile.open(name, std::ios::out | std::ios::trunc );

  std::cout << "Starting checkpoint " << name << std::endl;

  const char *serialized = reinterpret_cast<const char*>(destination); 
  outfile.write(serialized, sizeof(float) * num_elements);

  std::cout << "Done checkpoint " << checkpoint_index << std::endl;

  outfile.close();
}

int main(int argc, char **argv) {
  if (argc != 4 && argc != 5) {
    fprintf(stderr, "Usage: avg num_elements_per_proc num_rounds checkpoint_interval\n");
    exit(1);
  }

  int64_t num_elements_per_proc = atol(argv[1]);
  int64_t num_rounds = atol(argv[2]);
  int64_t checkpoint_interval = atol(argv[3]);
  int64_t fail_at = -1;
  if (argc == 5) {
      fail_at = atol(argv[4]);
  }
  bool use_noop = false;
 
  int world_rank, world_size;
  // MPI_Init(&argc, &argv);
  MPI_Init(NULL, NULL);
  print_mpi_stuff();

  MPI_Op mpi_noop;
  MPI_Op_create((MPI_User_function *)noop, 1, &mpi_noop);

  MPI_Op allreduce_op = use_noop? mpi_noop : MPI_SUM;

  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);

  // Get the name of the processor
  char processor_name[MPI_MAX_PROCESSOR_NAME];
  int name_len;
  MPI_Get_processor_name(processor_name, &name_len);

  // Create a random array of elements on all processes.
  srand(time(NULL) *
        world_rank); // Seed the random number generator of processes uniquely
  float *rand_nums = NULL, *destination = NULL;
  rand_nums = create_rand_nums(num_elements_per_proc);
  destination = (float *)malloc(sizeof(float) * num_elements_per_proc);

  const auto Run = [=](bool root_verbose, int64_t i) {
    MPI_Barrier(MPI_COMM_WORLD);
    double start, end;
    start = MPI_Wtime();
    MPI_Allreduce(rand_nums, destination, num_elements_per_proc, MPI_FLOAT,
                  allreduce_op, MPI_COMM_WORLD);
    if (fail_at > 0 && i == fail_at) {
        // Sleep for 2s to mimic failure detection.
        usleep(2 * 1000 * 1000);
        exit(1);
    }
    if (i % checkpoint_interval == 0 && i > 0) {
      SaveCheckpoint(rand_nums, num_elements_per_proc, i);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    end = MPI_Wtime();

    if (root_verbose && world_rank == 0) {
      auto time_point = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());
      auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(time_point.time_since_epoch());
      // Num float32 per proc, Num proc, Wallclock in ms.
      std::cout << i << "," << std::to_string((end - start) * 1e3) << "," << milliseconds.count() << std::endl;
    }

    std::memcpy(rand_nums, destination, sizeof(float) * num_elements_per_proc);
  };

  //const int kWarmups = 3;
  //for (int i = 0; i < kWarmups; ++i) {
  //  Run(/*root_verbose=*/false);
  //}

  int64_t checkpoint_index = 0;
  while (CheckpointExists((checkpoint_index + 1) * checkpoint_interval)) {
    checkpoint_index++;
  }

  int64_t i = 0;
  if (checkpoint_index > 0) {
    i = checkpoint_index * checkpoint_interval;
    LoadCheckpoint(rand_nums, num_elements_per_proc, i);
  }

  std::cout << "Starting from round " << i << std::endl;
  for (; i < num_rounds; ++i) {
    Run(/*root_verbose=*/true, i);
  }

  // Clean up
  free(rand_nums);
  free(destination);

  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Finalize();
}
