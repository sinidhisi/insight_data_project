from pyspark.sql import SparkSession
from pyspark import SparkContext ,SparkConf

spark = SparkSession.builder.appName("gdelt").getOrCreate()
conf = SparkConf().setAppName("gdelt")#.setMaster(master)
sc = SparkContext()

lines = sc.textFile("s3a://gdelt-open-data/events/2018*") # Loads 73,385,698 records from 2016
# Split lines into columns; change split() argument depending on deliminiter e.g. '\t'
parts = lines.map(lambda l: l.split('\t'))
# Convert RDD into DataFrame
from urllib import urlopen
html = urlopen("http://gdeltproject.org/data/lookups/CSV.header.dailyupdates.txt").read().rstrip()
columns = html.split('\t')
df = spark.createDataFrame(parts, columns)
df.printSchema()
df.select("sqldate").show()
sc.stop()
spark.stop()
