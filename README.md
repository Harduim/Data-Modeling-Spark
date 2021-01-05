# Data Lake with Apache Spark and Amazon S3 | Udacity Project 04

Main goals of this project:  
- Migrate files on S3 from JSON to Parquet Pyspark.
- Migrated data should be in a star schema


## Project Description
A fictional music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. 
Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

Build an ETL pipeline that extracts data from S3, processes using Spark, and loads the data back into S3 as a set of dimensional tables.
This will allow their analytics team to continue finding insights in what songs their users are listening to.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Project Datasets

### Song Dataset 

The first dataset is a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/). 
Each file is in JSON format and contains metadata about a song and the artist of that song.

#### s3://udacity-dend/song_data

### Log Dataset
The second dataset consists of log files in JSON format generated by an event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.

#### s3://udacity-dend/log_data

## Code style

### Formatting 
#### Using [Black](https://github.com/psf/black) with line lengh set to 100 `black -l 100 script.py`

#### Imports organized with [isort](https://pypi.org/project/isort/) with line lengh set to 100 `isort -l 100 script.py `