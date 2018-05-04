import os
# add dependency to use spark with kafka
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 pyspark-shell'
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import split
import config
import json, math, datetime
from pyspark.sql import DataFrameWriter
from pyspark.sql import SQLContext

def sendPostgres(df):
     
    print("send to postGres")
    my_writer = DataFrameWriter(df)

    url = 'jdbc:postgresql://ec2-54-70-242-121.us-west-2.compute.amazonaws.com:5432/postgres'
    
    properties = {
    	 "user": "nidhi",
   	 "password": "nidhi"
  	}
    table = 'temp'
    #df = SQLContext.read.jdbc(url=url, table=table, properties=properties)

    mode = "overwrite"
    df.write.jdbc(url=url, table="temp", mode=mode, properties=properties)

    my_writer.jdbc(url_connect, table, mode, properties)

    #sql = """INSERT INTO temp(data)
    #         VALUES(%s) RETURNING id1;"""

    sql = """SELECT * FROM temp;"""
    #cur = conn.cursor()
    #cur.execute(sql)
    #id1 = cur.fetchone()[0]
    #conn.commit()
    
    #cur.close() 

    #conn.close()

def main(): 

    sc = SparkContext(appName="PythonSparkStreamingKafka")
    sc.setLogLevel("WARN")
    sqlc = SQLContext(sc)
    print "creating ssc"
    # set microbatch interval as 10 seconds
    ssc = StreamingContext(sc, 10)
    print " done creating ssc"
    #ssc.checkpoint(config.CHECKPOINT_DIR)

    # create a direct stream from kafka without using receiver
    kafkaStream = KafkaUtils.createDirectStream(ssc, ['gdelt_topic'], {"metadata.broker.list":'localhost:9092' })
    parts = kafkaStream.map(lambda l: l[1].split('\t'))
    print "message"
    parts.pprint()
    sendPostgres(parts) 
    #parts.foreachRDD(sendPostgres)
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

    # parse each record string as json
    #data_ds = kafkaStream.map(lambda v: json.loads(v[1]))
    #data_ds.count().map(lambda x:'Records in this batch: %s' % x)\
    #               .union(data_ds).pprint()


    # use the window function to group the data by window
    #dataWindow_ds = data_ds.map(lambda x: (x['userid'], (x['acc'], x['time']))).window(10,10)

    '''    
    calculate the window-avg and window-std
    1st map: get the tuple (key, (val, val*val, 1)) for each record
    reduceByKey: for each key (user ID), sum up (val, val*val, 1) by column
    2nd map: for each key (user ID), calculate window-avg and window-std, return (key, (avg, std)) 
    '''
'''
    dataWindowAvgStd_ds = dataWindow_ds\
           .map(getSquared)\
           .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2]))\
           .map(getAvgStd)
    
    # join the original Dstream with individual record and the aggregated Dstream with window-avg and window-std 
    joined_ds = dataWindow_ds.join(dataWindowAvgStd_ds)

    # label each record 'safe' or 'danger' by comparing the data with the window-avg and window-std    
    result_ds = joined_ds.map(labelAnomaly)
    resultSimple_ds = result_ds.map(lambda x: (x[0], x[1], x[5]))

    # Send the status table to rethinkDB and all data to cassandra    
    result_ds.foreachRDD(lambda rdd: rdd.foreachPartition(sendCassandra))
    resultSimple_ds.foreachRDD(sendRethink)
   '''
if __name__ == '__main__':
    main()
