from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, NGram, HashingTF, MinHashLSH
from pyspark.sql.functions import col
from spark_session_builder import build_spark_session

import re

class LicensePattern:
    cc_pattern = re.compile("http[s]?://creativecommons\\.org/licenses/(by|by-sa|by-nd|by-nc|by-nc-sa|by-nc-nd|publicdomain)[\"/ >]")

def detect_licence(html:str):
    """
    Given a HTML string, this function detects the licence of the page.
    It returns a string with the licence name, or NO-LICENCE-FOUND if no licence is found.
    """
    license_attribute_pattern = re.compile(LicensePattern.cc_pattern)

    # storing counts of all difference occurrences of link to CC
    multiple_occurrences_map = {}

    # add all of them to the list
    for match in license_attribute_pattern.finditer(html):
        licence = match.group(1)

        # add entry
        if licence not in multiple_occurrences_map:
            multiple_occurrences_map[licence] = 0

        # and increase count
        multiple_occurrences_map[licence] += 1

    # no licence found
    if not multiple_occurrences_map:
        return "no-licence-found"

    # only one link found or if multiple links found but the same type
    if len(multiple_occurrences_map) == 1:
        return list(multiple_occurrences_map.keys())[0]

    # if multiple different links found, we return a general CC-UNSPECIFIED
    return "cc-unspecified" 



from langdetect import detect



def english_checking(text):
    """
    Given a text, this function detects if the text is in English.
    It returns a boolean.
    """
    try:
        if detect(text) == "en":
            return "english"
        else:
            return "non-english"
    except:
        return "non-english"

print(english_checking("hello world how are you"))
# spark = build_spark_session("spark://cpu128-dy-r6i-32xlarge-46:7077", 64, 512)

# db = spark.read.parquet("/fsx/shared/pilev2_parquet/StackExchange_ver4_non_local_dedupped/dataset.parquet")

# # Define the regular expression pattern to match HTML/JavaScript code
# html_js_pattern = "<.*?>"

# # Replace the HTML/JavaScript code with an empty string in the text column
# df_cleaned = db.withColumn("text", regexp_replace(db["text"], html_js_pattern, ""))

# # Show the cleaned DataFrame
# df_cleaned.show()



#scaling up #########
import re

def clean_html_tags(text):
    text = re.sub(r'<[^>]+>', '', text)
    text = '\n'.join([line.strip() for line in text.splitlines() if len(line) > 5])
    text = text.replace("&nbsp;", "")
    text = text.replace("&lt;", "<")
    return text


from pyspark.sql.functions import udf
spark = build_spark_session("spark://cpu128-dy-c6i-32xlarge-1:7077", 64, 256)


import boto3
s3 = boto3.resource("s3")
my_bucket = s3.Bucket("s-laion")
file_paths = []
for my_bucket_object in my_bucket.objects.filter(Prefix="bild_text/run1/2023-02-07-23-32-48/part_1/"):
    file_paths.append(f"s3a://s-laion/{my_bucket_object.key}")
#print(file_paths[0:10])
# file_paths = file_paths[1:3]    
#file_paths = file_paths[0:10]
data = spark.read.parquet(*file_paths)

find_license_udf = udf(detect_licence)

data = data.withColumn("license", find_license_udf(data["text"]))

stats = data.groupBy("license").count().orderBy("count", ascending=False)
stats.show()
exit()
find_english = udf(english_checking)
data = data.withColumn("english", find_english(data["text"]))

stats = data.groupBy("english").count().orderBy("count", ascending=False)
stats.show()

# clean_html_udf = udf(clean_html_tags)
# df_cleaned = df.withColumn("cleaned_text", clean_html_udf(df["text"]))
# df_cleaned.write.parquet("cleaned_file.parquet")
