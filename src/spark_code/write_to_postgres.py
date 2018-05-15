import psycopg2

conn = psycopg2.connect(host="localhost",
                        port="5432",
                        user="postgres",
                        password="postgres",
                        database="postgres")

cursor = conn.cursor()

cursor.execute("CREATE TABLE NEW (id integer) ") 
#df.rdd.map(row, cursor.execute("INSERT INTO Table (ISIN) VALUES", (row.ISIN)))

conn.commit()
cursor.close()
conn.close()
