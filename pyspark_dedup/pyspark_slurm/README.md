### Setup Spark Cluster on SLURM

This is a guide to setup a Spark cluster on SLURM. The guide is based on the following resources:

1. Download and install Spark:
```bash
bash setup_spark.sh
```

2. Change sbatch configuration in `spark_on_slurm.sh` to match your cluster configuration.

3. Submit sbatch job:
```bash
sbatch spark_on_slurm.sh
```
After that getting the cluster head ip e.g. `cpu128-dy-r6i-32xlarge-46` and port `7077` from the output of the sbatch job, you can connect to the cluster using the following script:
```python
from spark_session_builder import build_spark_session
spark = build_spark_session("spark://cpu128-dy-r6i-32xlarge-46:7077", 64, 512)
```

4. In client node to run deduplication, you need to install pyspark with the same version as the cluster. You can use the following script to install pyspark:
```bash
pip install pyspark==3.3.1
```

5. Run the deduplication script:
```bash
python minhash_spark.py
```