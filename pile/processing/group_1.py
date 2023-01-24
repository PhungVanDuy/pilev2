import argparse

import numpy as np

from datasets import load_from_disk
from pathlib import Path
from squeakily.core import Pipeline
from squeakily.clean import fix_utf8_encoding
from squeakily.filter import check_compression_ratio

# Parse the arguments
parser = argparse.ArgumentParser()
parser.add_argument(
    "--data_dir",
    type=str,
    required=True,
    help="The directory where the data is stored.",
)
parser.add_argument(
    "--output_dir",
    type=str,
    required=True,
    help="The directory where the output should be stored.",
)
parser.add_argument(
    "--min_percentile",
    type=float,
    default=0.01,
    help="The minimum percentile to use for the threshold.",
)
parser.add_argument(
    "--num_proc",
    type=int,
    default=32,
    help="The number of processes to use for the filtering.",
)
parser.add_argument(
    "--num_files_per_shard",
    type=int,
    default=10_000,
    help="The number of files per shard.",
)

args = parser.parse_args()
data_dir = Path(args.data_dir)
output_dir = Path(args.output_dir)
output_dir.mkdir(parents=True, exist_ok=True)
datasources = [
    {
        "dataset": load_from_disk(args.data_dir, keep_in_memory=False),
        "name": data_dir.name,
        "columns": ["text"],
        "filters": [check_compression_ratio],
        "cleaners": [fix_utf8_encoding],
    }
]
pipeline = Pipeline(datasources)
pipeline.run(dry_run=True, num_proc=96)
new_ds = pipeline.datasources[0]["dataset"]
start_size = len(new_ds)
compression_ratios = new_ds["check_compression_ratio_criteria"]
min_compression_ratio = np.quantile(compression_ratios, args.min_percentile)
new_ds = new_ds.filter(
    lambda x: x["check_compression_ratio_criteria"] > min_compression_ratio,
    batched=True,
    num_proc=32,
)
num_shards = len(new_ds) // args.num_files_per_shard
if num_shards == 0:
    num_shards = 1
ds_shards = [new_ds.shard(num_shards, i, contiguous=True) for i in range(num_shards)]
for i, shard in enumerate(ds_shards):
    path = output_dir / f"{data_dir.name}_shard_{i}.jsonl.zst"
    shard.to_json(
        path,
        lines=True,
        orient="records",
    )