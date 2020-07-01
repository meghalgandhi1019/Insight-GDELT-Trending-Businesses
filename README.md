![Python 3.8](https://img.shields.io/badge/python-3.7-green.svg)
![Spark](https://img.shields.io/badge/Spark-2.4.5-green)

# Trendustries
Using GDELT events data to provide trending of the business sectors.


[Trendustries](https://docs.google.com/presentation/d/1Ac4zOok6FbNJ7UfcnFMbY5wfHUFLPWu_-mNgvXp4lzs/edit?usp=sharing) presentation.

<hr/>

## Required installation, tools up and running
- Apache Spark
- PostgreSQL Database
- Grafana
- Airflow
- Python
- pandas
- Parquet
- Amazon EC2 instances
- Amazon S3

<hr/>

## Introduction
Starting a new business in a particular business sector is a challenge for anyone who wants to make it highly profitable. Knowing the recent & historical trending of the business sectors in a given location is critical (For the purpose of this project, location refers to a state). Trendings of the business sectors directs anyone to start successful business in a particular location. The goal of this project is to provide these business sector trendings to the user in a selected location which helps him to convert it into a moneymaking business. 


## Dataset
Global Database of Events, Languages and Tone (GDELT).
The GDELT project collects news stories from print and web sources from around the world. It's able to identify business sectors, organizations, and business events that are driving the global soceity. 

## Architecture
![Image](img/pipeline_v2.png)
GDELT's historical data exists in an Amazon S3 bucket. An offline Apache Spark batch processing job reads the data from S3, writes in a parquet format to S3 bucket and again loads data from it perform various Spark functions on it. The processed data is saved in a PostgreSQL database. Grafana is the user facing component for this architecture. The user is able to specify a state, and a business sector they're interested in. Grafana makes the appropriate queries to the PostgreSQL database. The results are viewed on the Grafana dashboard.

New GDELT updates are acquired from the source. A Python script uploads the new raw data to S3 followed by above mentioned spark job yo process it. GDELT updates are posted every 15 minutes, but trending results will be updated after every month and so Airflow workflow is scheduled to complete this process on new data in a monthly basis.


## Engineering challenges
- **Frequent shuffle and sort among workers while joining**: In this project, two different sized data frames(small data frame: events, large data frame: mentions) are being used for spark joining. Spark was shuffling lot of data from large sized data frame while performing traditional join, which has increased network traffic and slowed down the joining. To avoid these shuffling, **broadcast join** is implemented in this project. It has avoided sending all data of the large data frame over the network. Spark can 'broadcast' a small data frame by sending all the data in that small DataFrame to all nodes in the cluster. After the small DataFrame is broadcasted, Spark has performed a join without shuffling any of the data in the large DataFrame. It has improved the spark job computation time by ~5 mins.

- **Poor performance while writing parquet files to S3 bucket**: Amazon S3 doesn't have hadoop daemons to handle parquet file efficiently while writing, which slowed down writing. It took around 130 mins to write initially. But after changing **'spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version'** to **'2'**, it took 7-8 mins to write parquet files to s3. Spark is using complex file renaming policy with default algorithm 1. Algorithm version 1 has two phases of rename: one to commit the individual task output, and the other to commit the overall job output from completed/successful tasks. Algorithm version 2 is more efficient because task commits rename files directly to the final output location. This eliminates the second rename phase. It has improved the spark s3 parquet writing speed by ~20x. 


