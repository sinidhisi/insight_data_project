import psycopg2
def ps():
    

    v1 = 'testing_name'
    v2 = 'testing_id'


    conn = psycopg2.connect(host="localhost",
                        port="5432",
                        user="postgres",
                        password="postgres",
                        database="postgres")

    cursor = conn.cursor()
    conn.commit()
    cursor.close()
    conn.close()

def append_to_table(row):
    cursor.execute("INSERT INTO Table (customerName, customerId) VALUES(%s, %s)", (row.Name, row.Id))

#df.rdd.map(append_to_table)

    
def sendPostgres():
     
    print("send to postGres")
    #my_writer = DataFrameWriter(df)

    url = 'jdbc:postgresql://ec2-54-70-242-121.us-west-2.compute.amazonaws.com:5432/postgres'
    
    properties = {
        "user": "nidhi",
        "password": "nidhi"
      }
    table = 'temp'
    df1 = SQLContext.read.jdbc(url=url, table=table, properties=properties)

    mode = "overwrite"
    df.write.jdbc(url=url, table="temp", mode=mode, properties=properties)

    