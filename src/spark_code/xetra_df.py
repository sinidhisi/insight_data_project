import pyspark
import os
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import col

def ps():
    import psycopg2

    v1 = 'testing_name'
    v2 = 'testing_id'


    conn = psycopg2.connect(host="ec2-54-70-242-121.us-west-2.compute.amazonaws.com",
                        port="5432",
                        user="nidhi",
                        password="nidhi",
                        database="postgres")

    cursor = conn.cursor()

def append_to_table(row):
    cursor.execute("INSERT INTO customerTable (customerName, customerId) VALUES(%s, %s)", (row.customerName, row.customerId))

df.rdd.map(append_to_table)
conn.commit()
cursor.close()
conn.close()
    
def sendPostgres(df):
     
    print("send to postGres")
    #my_writer = DataFrameWriter(df)

    url = 'jdbc:postgresql://ec2-54-70-242-121.us-west-2.compute.amazonaws.com:5432/postgres'
    
    properties = {
        "user": "nidhi",
        "password": "nidhi"
      }
    table = 'temp'
    #df = SQLContext.read.jdbc(url=url, table=table, properties=properties)

    mode = "overwrite"
    df.write.jdbc(url=url, table="temp", mode=mode, properties=properties)

    

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
# average
#df.groupby('ISIN').agg({'MaxPrice': 'mean'}).show()
# distinct
sendtoPostgres(df.select('SecurityDesc').distinct())
#print(df.select('ISIN').distinct().count())
#print(df.select('Mnemonic').distinct().count())

df.show()

#df.show(10)
#df.printSchema()
            
#print(lines.count())

spark.stop()