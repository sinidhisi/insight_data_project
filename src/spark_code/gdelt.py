
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
import sys

date = ''.join(sys.argv[1].split('-'))
word = sys.argv[2]
gdeltFile = "s3://gdelt-open-data/events/"+date+"*" # read from S3 

spark = SparkSession.builder.appName("gdelt_analytics").getOrCreate()

gdeltData = spark.read.csv(gdeltFile).cache()
gdelt_format = gdeltData.map(lambda line: line.split("\t"))

Event = Row('globaleventid' ,'day' ,'monthyear' ,'year' ,'fractiondate' ,'actor1code' ,'actor1name' ,'actor1countrycode' ,'actor1knowngroupcode' ,'actor1ethniccode' ,'actor1religion1code' ,'actor1religion2code' ,'actor1type1code' ,'actor1type2code' ,'actor1type3code' ,'actor2code' ,'actor2name' ,'actor2countrycode' ,'actor2knowngroupcode' ,'actor2ethniccode' ,'actor2religion1code' ,'actor2religion2code' ,'actor2type1code' ,'actor2type2code' ,'actor2type3code' ,'isrootevent' ,'eventcode' ,'eventbasecode' ,'eventrootcode' ,'quadclass' ,'goldsteinscale' ,'nummentions' ,'numsources' ,'numarticles' ,'avgtone' ,'actor1geo_type' ,'actor1geo_fullname' ,'actor1geo_countrycode' ,'actor1geo_adm1code' ,'actor1geo_lat' ,'actor1geo_long' ,'actor1geo_featureid' ,'actor2geo_type' ,'actor2geo_fullname' ,'actor2geo_countrycode' ,'actor2geo_adm1code' ,'actor2geo_lat' ,'actor2geo_long' ,'actor2geo_featureid' ,'actiongeo_type' ,'actiongeo_fullname' ,'actiongeo_countrycode' ,'actiongeo_adm1code' ,'actiongeo_lat' ,'actiongeo_long' ,'actiongeo_featureid' ,'dateadded' ,'sourceurl' )

event = gdelt_format.map(lambda r: Event(*r))

df2 = sqlContext.createDataFrame(event)
schema = StructType([
	...    StructField("name", StringType(), True),
	...    StructField("age", IntegerType(), True)])

#Event = Row('globaleventid' INT,'day' INT,'monthyear' INT,'year' INT,'fractiondate' FLOAT,'actor1code' string,'actor1name' string,'actor1countrycode' string,'actor1knowngroupcode' string,'actor1ethniccode' string,'actor1religion1code' string,'actor1religion2code' string,'actor1type1code' string,'actor1type2code' string,'actor1type3code' string,'actor2code' string,'actor2name' string,'actor2countrycode' string,'actor2knowngroupcode' string,'actor2ethniccode' string,'actor2religion1code' string,'actor2religion2code' string,'actor2type1code' string,'actor2type2code' string,'actor2type3code' string,'isrootevent' BOOLEAN,'eventcode' string,'eventbasecode' string,'eventrootcode' string,'quadclass' INT,'goldsteinscale' FLOAT,'nummentions' INT,'numsources' INT,'numarticles' INT,'avgtone' FLOAT,'actor1geo_type' INT,'actor1geo_fullname' string,'actor1geo_countrycode' string,'actor1geo_adm1code' string,'actor1geo_lat' FLOAT,'actor1geo_long' FLOAT,'actor1geo_featureid' INT,'actor2geo_type' INT,'actor2geo_fullname' string,'actor2geo_countrycode' string,'actor2geo_adm1code' string,'actor2geo_lat' FLOAT,'actor2geo_long' FLOAT,'actor2geo_featureid' INT,'actiongeo_type' INT,'actiongeo_fullname' string,'actiongeo_countrycode' string,'actiongeo_adm1code' string,'actiongeo_lat' FLOAT,'actiongeo_long' FLOAT,'actiongeo_featureid' INT,'dateadded' INT,'sourceurl' string)

gdeltData.take(10)
# count total No. of events on the given date

total_events =  gdeltData.count()

# print first line to get a feel for the data
print gdeltData.first()

# get the lines that have the word "word" in them

#linesWithWord = gdeltData.filter(gdeltData.value.contains(word))

# get the number of lines that have the word "word"  in them

#numLines = linesWithWord.count()

print "total events = ", total_events
#print "first 10 lines with word", word, " in it ", linesWithWord.take(10)
spark.stop()
