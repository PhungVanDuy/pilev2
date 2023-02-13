import boto3
s3 = boto3.resource("s3")
my_bucket = s3.Bucket("s-eai-neox")
file_paths = []
for my_bucket_object in my_bucket.objects.filter(Prefix="data/codepile/group1/"):
    file_paths.append(f"s3a://s-eai-neox/{my_bucket_object.key}")
print(file_paths[0])
from spark_session_builder import build_spark_session
# file_paths = file_paths[100:200]
spark = build_spark_session("spark://cpu128-dy-r6i-32xlarge-5:7077", 64, 256)
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")

data = spark.read.parquet(*file_paths)
# Convert column to string
data = data.withColumn("meta", data.meta.cast("string"))
import time
start = time.time()
#data.filter(data.meta.contains("Project Gutenberg")).show(5)
df = data.filter(data.meta.contains("Project Gutenberg"))
end = time.time()
print("Time to query: ", end-start)
start = time.time()
df.write.parquet("Gutenberg.parquet")
end = time.time()
print("Time to write: ", end-start)
exit()
# arXiv
print("arXiv")
data.filter(data.meta.contains("arXiv_out")).show(5)
# save to file
data.filter(data.meta.contains("arXiv_out")).write.parquet("arXiv.parquet")
# PubMed
print("PubMed")
data.filter(data.meta.contains("PubMedDataset")).show(5)
data.filter(data.meta.contains("PubMedDataset")).write.parquet("PubMed.parquet")
# FreeLaw Options
print("FreeLaw")
data.filter(data.meta.contains("case_jurisdiction")).show(5)
data.filter(data.meta.contains("case_jurisdiction")).write.parquet("FreeLaw.parquet")
# UbuntuIRC
print("UbuntuIRC")
data.filter(data.meta.contains("Ubuntu IRC")).show(5)
data.filter(data.meta.contains("Ubuntu IRC")).write.parquet("UbuntuIRC.parquet")
# Gutenberg
print("Gutenberg")
data.filter(data.meta.contains("Project Gutenberg")).show(5)
data.filter(data.meta.contains("Project Gutenberg")).write.parquet("Gutenberg.parquet")
# EnWiki
print("EnWiki")
data.filter(data.meta.contains("wikidata_id")).show(5)
data.filter(data.meta.contains("wikidata_id")).write.parquet("EnWiki.parquet")
# EuroParliamentProceedings
print("EuroParliamentProceedings")
data.filter(data.meta.contains("'language'")).show(5)
data.filter(data.meta.contains("'language'")).write.parquet("EuroParliamentProceedings.parquet")
# OtherWiki
print("OtherWiki")
data.filter(data.meta.contains("wiki_source")).show(5)
data.filter(data.meta.contains("wiki_source")).write.parquet("OtherWiki.parquet")
# S2ORC
print("S2ORC")
data.filter(data.meta.contains("S2ORC")).show(5)
data.filter(data.meta.contains("S2ORC")).write.parquet("S2ORC.parquet")
# USPTO
print("USPTO")
data.filter(data.meta.contains("USPTO")).show(5)
data.filter(data.meta.contains("USPTO")).write.parquet("USPTO.parquet")
# PileOfLaw
print("PileOfLaw")
data.filter(data.meta.contains("'dataset'")).show(5)
data.filter(data.meta.contains("'dataset'")).write.parquet("PileOfLaw.parquet")
