#!/cvmfs/soft.computecanada.ca/gentoo/2020/bin/bash
#SBATCH --account=def-queenspp
#SBATCH --nodes=32
#SBATCH --ntasks-per-node=64
#SBATCH --mem=0
#SBATCH --exclusive
#SBATCH --time=30:00
#SBATCH --output=%x-%j.txt
#SBATCH --error=%x-error-%j.txt


pushd /home/bkitor/Playground/bk_eval_sync
source initenv.bash

BK_EVAL_BIN=/home/bkitor/Playground/bk_eval_sync/bin/eval_ss

if [[ "" == $SLURM_JOBID ]]; then
	SLURM_NNODES=1
fi

# for BK_NPERNODE in $(seq 1 2 64); do
for BK_NPERNODE in 1 2 4 8 16 32 64; do
	for BK_EXP in 0 1; do
			BK_NPROC="$(($SLURM_NNODES * $BK_NPERNODE))"
			echo "-n $BK_NPROC -N $SLURM_NNODES -e $BK_EXP"
			mpirun -n $BK_NPROC \
				--map-by ppr:$BK_NPERNODE:node \
				$BK_EVAL_BIN -x 100 -i 1000 -e $BK_EXP
	done
done
