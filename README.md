# Sparkify Song Play S3 Data Lake processed by AWS EMR Spark Project

### ***Udacity Data Engineering Course 3: Spark and Data Lakes***
### ***Final Course Project Assignment***

***
A music streaming startup (Sparkify) stores all it's key event data on S3.
The data is well structured as a Data Lake.

This project implements an ETL pipeline to extract data from S3, then load it on a Spark Cluster, the data is then processed into analytic tables ready for downstream business/data analysts or other users, this tables are then loaded back to S3 as Parquet Files, ready for consumption.

The whole project is based on mainly AWS Boto3 Python SDK and psyspark library.
***
## Contents: 
1. Data to be used: the data is stored in a S3 Bucket
    1. Listening events for it's streaming data as JSON logfiles.
    2. Song data for all tracks available on it's service as JSON files. 
    
2. Python Scripts in Folder root and order of execution
    1. **create_EMR_cluster_test_connection.py** - provisions an EMR Spark Cluster, associates appropriate IAM roles with the Cluster, allows TCP connections and tests a connection to the Cluster using dwh.cfg parameters.<br>
    After running update the Endpoint in dl.cfg for all files to use.

    2. **etl.py** - loads data from S3 to EMR cluster, processes the data into analytic tables then exports the ready tables back to S3 as parquet files.

3. dl.cfg - Contains AWS credentials and EMR Cluster parameters.

***
### To run first create cluster, update dl.cfg, then run ETL.



### ***Obira Daniel, August 2022***

## Database Schema

![Database Schema](DB_Schema.png)
