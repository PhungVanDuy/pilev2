#!/bin/bash
#SBATCH --partition=cpu128
#SBATCH --job-name=spark_on_slurm
#SBATCH --nodes 3 
#SBATCH --ntasks-per-node 1
#SBATCH --cpus-per-task=12
#SBATCH --mem=0 # 0 means use all available memory (in MB)
#SBATCH --output=%x_%j.out
#SBATCH --comment carper 
#SBATCH --exclusive

srun --comment carper bash worker_spark_on_slurm.sh
