import boto3
s3 = boto3.resource("s3")
my_bucket = s3.Bucket("s-eai-neox")
file_paths = []
for my_bucket_object in my_bucket.objects.filter(Prefix="data/codepile/group1/"):
    file_paths.append(f"s3a://s-eai-neox/{my_bucket_object.key}")
print(file_paths[0])
from spark_session_builder import build_spark_session
# file_paths = file_paths[100:200]
spark = build_spark_session("spark://cpu128-dy-r6i-32xlarge-46:7077", 32, 256)
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")

data = spark.read.parquet(*file_paths)

# Convert column to string
all_groups = [
        "PileV2Reddit2019",
        "CodePileReddit2019",
        "PileV2Reddit2020",
        "CodePileReddit2020",
        "PileV2Reddit2021",
        "CodePileReddit2021",
        "PileV2Reddit2022",
        "CodePileReddit2022","PileV2Posts",
        "CodePilePosts", "StackExchange", "USENET", "Discourse", "GithubIssues",
        "Opensubtitles", "DevDocs"]

for group in all_groups:
    print(group)
    data.filter(data.meta.contains(group)).show(5)
    print("")