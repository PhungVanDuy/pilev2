import datasets
import logging
import argparse
import os
import json
import pathlib
from tqdm import tqdm

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_file", type=str)
    parser.add_argument("--output_file", type=str)
    args = parser.parse_args()

    minhash_dataset_path_dict = {}
    minhash_dataset_list_file = args.input_file
    with open(minhash_dataset_list_file, "r") as f:
        lines = f.read().splitlines()
        for line in tqdm(lines):
            minhash_dataset_path = line.strip()
            # check if the path is valid and is a directory
            assert os.path.isdir(minhash_dataset_path.strip())
            # check if the path is a minhash dataset (ends with _minhash)
            #assert minhash_dataset_path.strip().endswith("_minhash")
            # check if it is a hugging face dataset format by checking if it has a dataset_info.json
            assert os.path.isfile(os.path.join(minhash_dataset_path.strip(), "dataset_info.json"))
            print(f"Starting to load")
            dataset = datasets.load_from_disk(minhash_dataset_path.strip())
            print(f"Loaded the dataset")
            minhash_dataset_path_dict[minhash_dataset_path] = len(dataset)
    
    with open(args.output_file, "w") as f:
        json.dump(minhash_dataset_path_dict, f)
        print(minhash_dataset_path_dict)
        print(f"Wrote the file to {args.output_file}")