from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pathlib import Path
from typing import List
import pyarrow.parquet as pq
import pyarrow as pa

def convert_to_parquet(input_file: str, output_file: str):
    table = pa.ipc.read_table(input_file)
    pq.write_table(table, output_file, filesystem=pa.S3FileSystem())

def find_arrow_files(directory: str) -> List[str]:
    arrow_files = []
    for file in Path(directory).rglob('*.arrow'):
        arrow_files.append(str(file))
    return arrow_files

def convert_to_parquet_udf(input_file: str, output_file: str):
    convert_to_parquet(input_file, output_file)

# convert_to_parquet_udf_spark = udf(convert_to_parquet_udf, StringType())

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Convert to Parquet").getOrCreate()

    # Find all arrow files in the directory
    root = "/fsx/shared/pilev2/pilev2_group2/"
    arrow_files = find_arrow_files(root)
    # create output files with the same path as the input files but in s3 
    output_files = [f"s3://s-eai-neox/data/codepile/group2/{file[len(root):].replace('.arrow', '.parquet')}" for file in arrow_files]

    # Create DataFrame from the list of arrow files
    data = spark.createDataFrame(zip(arrow_files, output_files), ["input_file", "output_file"])

    # Iterate over the DataFrame and call the UDF for each row
    data.rdd.map(lambda x: convert_to_parquet_udf(x["input_file"], x["output_file"])).collect()