"GDELT dataset found here: https://aws.amazon.com/public-datasets/gdelt/
# Column headers found here: http://gdeltproject.org/data/lookups/CSV.header.dailyupdates.txt

# Load RDD
from pyspark.sql import SparkSession
from pyspark import SparkContext ,SparkConf
#spark = SparkSession.builder.appName("gdelt").getOrCreate()
conf = SparkConf().setAppName("gdelt")#.setMaster(master)
sc = SparkContext(conf=conf)
lines = sc.textFile("s3a://gdelt-open-data/events/2018*") # Loads 73,385,698 records from 2016
# Split lines into columns; change split() argument depending on deliminiter e.g. '\t'
parts = lines.map(lambda l: l.split('\t'))
# Convert RDD into DataFrame
from urllib import urlopen
html = urlopen("http://gdeltproject.org/data/lookups/CSV.header.dailyupdates.txt").read().rstrip()
columns = html.split('\t')
df = sc.createDataFrame(parts, columns)
df.printSchema
sc.stop()
