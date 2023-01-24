#!/bin/bash
#SBATCH --job-name=Reddit_0
#SBATCH --output=./PileV2Reddit2021_part_0.o
#SBATCH --error=./PileV2Reddit2021_part_0.e
#SBATCH --mem=960GB
#SBATCH --cpus-per-task=128
#SBATCH --partition=cpu128
#SBATCH --comment=carper
#SBATCH --exclusive
#SBATCH --export=ALL
# ===== END SLURM OPTIONS =====
source /fsx/home-erfan/miniconda3/bin/activate pilev2
cd /fsx/home-erfan/pilev2/pile/processing/dedup
mkdir -p job_logs 
DATA_PATH=/fsx/shared/pilev2/deduped_2_2/deduped_2_2/group_2_2/shard_0
OUTPUT_PATH=/fsx/shared/pilev2_hashes/group_2
dataset=PileV2Reddit2021_part_0
python generate_min_hashes.py --dataset-path $DATA_PATH --column text --threshold 0.85 --output $OUTPUT_PATH/$dataset
mv PileV2Reddit2021_part_0.o job_logs/PileV2Reddit2021_part_0.o
mv PileV2Reddit2021_part_0.e job_logs/PileV2Reddit2021_part_0.e