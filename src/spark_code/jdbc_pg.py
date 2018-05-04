import pyspark
import os
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import col

def sendPostgres():
     
    print("send to postGres")
    #my_writer = DataFrameWriter(df)

#    url = 'jdbc:postgresql://localhost:5432/xetra'
    
    properties = {
        "user": "postgres",
        "password": "postgres"
      }
    table = 'temp'
    #df1 = SQLContext.read.jdbc(url=url, table=table, properties=properties)

    mode = "overwrite"
    #df.write.jdbc(url=url, table="temp", mode=mode, properties=properties)

#put in main later     
spark = pyspark.sql.SparkSession.builder \
    .master("local[*]") \
    .appName("Spark") \
    .config("spark.executor.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true") \
    .getOrCreate()

# Set the property for the driver. Doesn't work using the same syntax 
# as the executor because the jvm has already been created.
spark.sparkContext.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")

aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.eu-central-1.amazonaws.com")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_access_key)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_access_key)

lines = spark.sparkContext.textFile('s3a://deutsche-boerse-xetra-pds/2018-04-25*')

parts = lines.map(lambda l: l.split(','))\
        .filter(lambda part: len(part) == 14)

#print(parts.map(lambda x: (1,len(x))).countByValue())

columns = parts.take(1)[0]

df = spark.createDataFrame(parts, columns)
df.rdd.map(sendPostgres)
