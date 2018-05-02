from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "gdelt_topic") \
  .load()

df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Split the lines into words
words = df.select(
   explode(
       split(df.value, " ")
   ).alias("word")
)

# Generate running word count
wordCounts = words.groupBy("word").count()

#start running the query that prints the running counts to the console
query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
