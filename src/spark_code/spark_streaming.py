import os
# add dependency to use spark with kafka
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 pyspark-shell'
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
import config
import json, math, datetime
def main():

    sc = SparkContext(appName="PythonSparkStreamingKafka")
    sc.setLogLevel("WARN")
    print "creating ssc"
    # set microbatch interval as 10 seconds
    ssc = StreamingContext(sc, 10)
    print " done creating ssc"
    #ssc.checkpoint(config.CHECKPOINT_DIR)

    # create a direct stream from kafka without using receiver
    kafkaStream = KafkaUtils.createDirectStream(ssc, ['gdelt_topic'], {"metadata.broker.list":'localhost:9092' })
    parts = kafkaStream#.map(lambda l: l.split('\t'))
    print "message"
    parts.pprint()
    # Split the lines into words
    #words = kafkaStream.map(lambda v: json.loads(v[1]))
    #words.pprint()
# Generate running word count
    #wordCounts = words.groupBy("word").count()
    #wordCounts.pprint()
    #data_ds.count().map(lambda x:'Records in this batch: %s' % x)\
    #               .union(data_ds).pprint()

    ssc.start()
    ssc.awaitTermination()
    return
if __name__ == '__main__':
    main()
