INPUT_FILE = "/fsx/home-reshinth/work_2/pilev2/pile/processing/dedup/stages_dedup/test/test_cluster_file.txt" OUTPUT_FILE = "/fsx/home-reshinth/work_2/pilev2/pile/processing/dedup/stages_dedup/test/test_cluster_stream.json"
OUTPUT_JSON_FILE = "/fsx/home-reshinth/work_2/pilev2/pile/processing/dedup/stages_dedup/test/test_cluster_stream.json" python pile/processing/dedup/stages_dedup/clustering_stream.py --minhash-dataset-list-file $OUTPUT_JSON_FILE
MINHASH_OUTPUT = "/fsx/home-reshinth/work_2/pilev2/pile/processing/dedup/stages_dedup/test/test_cluster_stream.json" python pile/processing/dedup/stages_dedup/clustering_stream.py --minhash-dataset-list-file $MINHASH_OUTPUT

python pile/processing/dedup/stages_dedup/calc_len_per_shard_file.py --input_file INPUT_FILE --output_file OUTPUT_FILE
python pile/processing/dedup/stages_dedup/clustering_stream.py --minhash-dataset-list-file MINHASH_OUTPUT