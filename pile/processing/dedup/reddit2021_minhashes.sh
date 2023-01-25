#! /bin/bash

# Set variables
DATA_PATH=/fsx/shared/pilev2/deduped_2_2/deduped_2_2/group_2_2
OUTPUT_PATH=/fsx/shared/pilev2_hashes/group_2/PileV2Reddit2021_ver2

# list the shards in an array from 1 to 10
shards=(new_shard_1 new_shard_2 new_shard_3 new_shard_4 new_shard_5 new_shard_6 new_shard_7 new_shard_8 new_shard_9 new_shard_10)
# Loop through shards and submit slurm job for each shard
for shard in ${shards[@]}; do
    DATA_PATH=/fsx/shared/pilev2/deduped_2_2/deduped_2_2/group_2_2
    DATA_PATH=$DATA_PATH/$shard
    mem=128GB
    cpus=64
    partition=cpu64
    temp_sbatch=./temp_sbatch.slurm
    shard_num=$(echo $shard | cut -d'_' -f3)
    cat << HELP > $temp_sbatch
#!/bin/bash
#SBATCH --job-name=Reddit_$shard_num
#SBATCH --output=./logs/Reddit_2021_$shard_num.o
#SBATCH --error=./logs/Reddit_2021_$shard_num.e
#SBATCH --mem=$mem
#SBATCH --cpus-per-task=$cpus
#SBATCH --partition=$partition
#SBATCH --exclusive
#SBATCH --comment=carper
#SBATCH --export=ALL
# ===== END SLURM OPTIONS =====
source /fsx/home-erfan/miniconda3/bin/activate pilev2
cd /fsx/home-erfan/pilev2/pile/processing/dedup
python generate_min_hashes.py --dataset-path $DATA_PATH --column text --threshold 0.85 --output $OUTPUT_PATH/$shard

HELP
    sbatch $temp_sbatch
    rm $temp_sbatch
done
