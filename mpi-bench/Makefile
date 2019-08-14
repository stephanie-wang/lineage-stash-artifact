EXECS=allreduce

MPICPP_OPENMPI?=mpicxx.openmpi

all: ${EXECS}

allreduce: allreduce.cc
	${MPICPP_OPENMPI} -O3 -std=c++11 -o allreduce allreduce.cc -lmpi

clean:
	rm -f ${EXECS}
