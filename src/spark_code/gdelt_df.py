from pyspark.sql import SparkSession
from pyspark import SparkContext ,SparkConf

spark = SparkSession.builder.appName("gdelt").getOrCreate()
conf = SparkConf().setAppName("gdelt")#.setMaster(master)
#sc = 

#lines = spark.sparkContext.textFile("s3a://gdelt-open-data/events/20180425*") 
lines = spark.read.text("s3a://gdelt-open-data/events/20160425*")
# Split lines into columns; change split() argument depending on deliminiter e.g. '\t'

#parts = lines.map(lambda l: l.split('\t'))

#numAs = lines.filter(lines.value.contains("TELEFONICA")).count()
numAs =lines.filter(lines.value.contains("TELEFONICA")).show(20, False)

print("Lines with a", numAs)

'''
# Convert RDD into DataFrame
from urllib import urlopen
html = urlopen("http://gdeltproject.org/data/lookups/CSV.header.dailyupdates.txt").read().rstrip()
columns = html.split('\t')
df = spark.createDataFrame(parts, columns)
df.printSchema()
df.select("sqldate").show()
'''
spark.stop()