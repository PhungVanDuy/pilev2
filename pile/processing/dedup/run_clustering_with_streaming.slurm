#!/bin/bash
#SBATCH --job-name=PileV2_G2Dedup
#SBATCH --output=./PileV2Clustering.o
#SBATCH --error=./PileV2Clustering.e
#SBATCH --mem=960GB
#SBATCH --cpus-per-task=128
#SBATCH --partition=cpu128
#SBATCH --comment=carper
#SBATCH --exclusive
#SBATCH --export=ALL
# ===== END SLURM OPTIONS =====
#TODO: replace with ur environment
source /fsx/home-erfan/miniconda3/bin/activate pilev2
#TODO: replace with ur path
cd /fsx/home-erfan/pilev2/pile/processing/dedup
mkdir -p job_logs 
INPUT_FILE=/fsx/home-erfan/pilev2/pile/processing/dedup/group2_hashes.txt
OUTPUT_JSON=/fsx/home-erfan/pilev2/pile/processing/dedup/group2_stream.json
python calc_len_per_shard_file.py --input_file $INPUT_FILE --output_file $OUTPUT_JSON
python clustering_stream.py --minhash-dataset-list-file $OUTPUT_JSON --bash-size 100000
mv PileV2Clustering.o job_logs/PileV2Clustering.o
mv PileV2Clustering.e job_logs/PileV2Clustering.e