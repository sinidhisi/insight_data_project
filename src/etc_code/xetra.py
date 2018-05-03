from pyspark.sql import SparkSession
import sys
spark = SparkSession.builder.appName("xetra_analytics").getOrCreate()


date = sys.argv[1]
word = sys.argv[2]

gdeltFile = "s3a://deutsche-boerse-xetra-pds/"+date+"*" # read from S3 


gdeltData = spark.read.text(gdeltFile).cache()

# count total No. of events on the given date

total_events =  gdeltData.count()

# print first line to get a feel for the data
print gdeltData.first()

# get the lines that have the word "word" in them

linesWithWord = gdeltData.filter(gdeltData.value.contains(word))

# get the number of lines that have the word "word"  in them

numLines = linesWithWord.count()

print "total events = ", total_events
print "total number of lines with word",word," in it ", numLines
print "first 10 lines with word", word, " in it ", linesWithWord.take(5)

spark.stop()
