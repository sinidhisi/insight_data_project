# InsightProject


vvStreaming analysis of stocks for fun and profit



## Purpose and Use cases

To study how the market behaves and monitor it in real time. Make predictions of stock prices in real-time and compare them with the real market data. User can login and do 3 things:

1. Check out how the market is doing in real time fashion. Which stocks are trending in last few minutes (or hours). What top news stories are relevant to the market.

2. Check out the trends for a specified stock. What news stories are relevant for that stock. Its behaviour in real time and historically.

3. Make predictions for the given stock. Could it go up or down the next day (This can be rethought of as check the offset of real behavior from  predictions).

## Dashboard
![Dashboard](https://github.com/sinidhisi/insight_data_project/blob/master/images/dashboard.jpg)




## Technologies

Lambda Architecture **?**

* **Data:** S3 [Deutche](https://registry.opendata.aws/deutsche-boerse-pds/), [GDELT](https://registry.opendata.aws/gdelt/),  APIs 	 

* **Data ingestion:** Kafka (with two consumers - consumer 1 will store the data onto hdfs and consumer 2 will feed it into spark streaming for analysis)

* **Data storage:** hdfs, S3

* **Batch processing:** Spark, Flink

* **Machine Learning:** SparkMLlib, Flinklib
 Incremental ML? (retrain the model everynight. Can see the change in parameters as more data comes in. Can also try to use some kind of incremental ML)

* **Stream processing:** Spark, Flink, Storm, Druid, Ksql
 
* **Scheduling:** Airflow

* **Visualization:** Superset

## What are the primary engineering challenges?

* Scaling
* Huge data (2.5 TB per day)    

## What are the (quantitative) specifications/constraints for this project?
 


## Proposed architecture
![architecture](https://github.com/sinidhisi/insight_data_project/blob/master/images/architecture.jpg)




