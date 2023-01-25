from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, NGram, HashingTF, MinHashLSH
from pyspark.sql.functions import col
from spark_session_builder import build_spark_session

spark = build_spark_session("spark://cpu128-dy-r6i-32xlarge-46:7077", 64, 512)

db = spark.read.parquet("/fsx/shared/pilev2_parquet/StackExchange_ver4_non_local_dedupped/dataset.parquet")
db.show(5)

model = Pipeline(stages=[
    RegexTokenizer(
        pattern="[^A-Za-z_0-9]", inputCol="text", outputCol="tokens", minTokenLength=1
    ),
    NGram(n=5, inputCol="tokens", outputCol="ngrams"),
    HashingTF(inputCol="ngrams", outputCol="vectors"),
    MinHashLSH(inputCol="vectors", outputCol="lsh", numHashTables=13)
]).fit(db)

db_hashed = model.transform(db)
db_hashed.show(5)

duplicates = model.stages[-1].approxSimilarityJoin(
    db_hashed,
    db_hashed,
    0.15,
    distCol="JaccardDistance"
).select(
    col("datasetA.id").alias("idA"),
    col("datasetB.id").alias("idB"),
    col("JaccardDistance")
)

# show first 10 duplicates
duplicates.show(10)
exit()

duplicates = duplicates.filter("idA = idB")
duplicates = duplicates.filter("idA < idB")
duplicates_ids = duplicates.select("idA").distinct().collect()
duplicates = duplicates.filter("datasetA.id < datasetB.id")