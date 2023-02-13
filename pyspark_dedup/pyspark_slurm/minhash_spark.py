from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, NGram, HashingTF, MinHashLSH
from pyspark.sql.functions import col
from spark_session_builder import build_spark_session

spark = build_spark_session("spark://cpu128-dy-r6i-32xlarge-46:7077", 64, 512)

db = spark.read.parquet("/fsx/shared/pilev2_parquet/StackExchange_ver4_non_local_dedupped/dataset.parquet")
db.show(5)


start = time.time()
db.cache()

# Use the "toLocalIterator()" method to retrieve the data from the RDD
rdd_data = [row for row in db.toLocalIterator()]

# Use the "parallelize()" method to parallelize the data
rdd = spark.sparkContext.parallelize(rdd_data, numSlices=5_000)

# Convert the RDD to a DataFrame
df = spark.createDataFrame(rdd, db.schema)

model = Pipeline(stages=[
    RegexTokenizer( # Stage 2
        pattern="[^A-Za-z_0-9]", inputCol="text", outputCol="tokens", minTokenLength=1
    ),
    NGram(n=5, inputCol="tokens", outputCol="ngrams"), # Stage 3
    HashingTF(inputCol="ngrams", outputCol="vectors"), # Stage 4
    MinHashLSH(inputCol="vectors", outputCol="lsh", numHashTables=13) # Stage 5
]).fit(df)

db_hashed = model.transform(df)

duplicates = model.stages[-1].approxSimilarityJoin(
    db_hashed,
    db_hashed,
    0.15,
    distCol="JaccardDistance"
).filter("datasetA.id < datasetB.id") # Stage 6
# duplicates.show()
duplicates.write.parquet("./duplicates", mode="overwrite") # Stage 7
end = time.time()
print(f"Time taken: {end - start} for {db.count()} rows")