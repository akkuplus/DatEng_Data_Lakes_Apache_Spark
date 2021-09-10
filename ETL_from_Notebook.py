#!/usr/bin/env python
# coding: utf-8

# # Setup to connect Spark with S3
# 
# printenv SPARK_HOME
# 
# -> cd /opt/spark-2.4.3-bin-hadoop2.7/conf
# 
# cp spark-defaults.conf.template spark-defaults.conf
# 
# apt-get install nano
# 
# nano spark-defaults.conf
# 
# """
# 
# spark.jars.packages                com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.2
# #spark.jars.packages				com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.7.3
# spark.hadoop.fs.s3a.access.key  	<>
# spark.hadoop.fs.s3a.secret.key  	<>
# spark.hadoop.fs.s3a.fast.upload 	true
# spark.hadoop.fs.s3n.impl            org.apache.hadoop.fs.s3native.NativeS3FileSystem
# spark.hadoop.fs.s3n.awsAccessKeyId 	<>
# spark.hadoop.fs.s3n.awsSecretAccessKey <>
# 
# """
# 
# 
# 

# # Test

import configparser
import logging as Log
import os
from pyspark.sql import SparkSession


config = configparser.ConfigParser()
#Normally this file should be in ~/.aws/credentials
config.read_file(open('dl.cfg'))
logger = Log.getLogger(__name__)
os.environ["AWS_ACCESS_KEY_ID"] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"] = config['AWS']['AWS_SECRET_ACCESS_KEY']


spark = SparkSession\
    .builder\
    .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.0")\
    .getOrCreate()
logger.debug("SparkSession")

logger.setLevel(Log.ERROR)




# # ETL


import configparser
from datetime import datetime
import os
import pyspark.sql.functions as F
import pyspark.sql.types as T
import s3fs


# Create schema for jsons of logs
LOG_SCHEMA = T.StructType([
    T.StructField("artist", T.StringType()),
    T.StructField("auth", T.StringType()),
    T.StructField("firstName", T.StringType()),
    T.StructField("gender", T.StringType()),
    T.StructField("itemInSession", T.LongType()),
    T.StructField("lastName", T.StringType()),
    T.StructField("length", T.DoubleType()),
    T.StructField("level", T.StringType()),
    T.StructField("location", T.StringType()),
    T.StructField("method", T.StringType()),
    T.StructField("page", T.StringType()),
    T.StructField("registration", T.DoubleType()),
    T.StructField("sessionId", T.LongType()),
    T.StructField("song", T.StringType()),
    T.StructField("status", T.LongType()),
    T.StructField("ts", T.LongType()),
    T.StructField("userAgent", T.StringType()),
    T.StructField("userId", T.StringType())
])

SONG_SCHEMA = T.StructType([
    T.StructField("artist_id", T.StringType(), True),
    T.StructField("artist_latitude", T.DoubleType(), True),
    T.StructField("artist_location", T.StringType(), True),
    T.StructField("artist_longitude", T.DoubleType(), True),
    T.StructField("artist_name", T.StringType(), True),
    T.StructField("duration", T.DoubleType(), True),
    T.StructField("num_songs", T.IntegerType(), True),
    T.StructField("song_id", T.StringType(), True),
    T.StructField("title", T.StringType(), True),
    T.StructField("year", T.IntegerType(), True),
])

input_data = os.path.join(os.getcwd(), "data")
# input_data = "s3a://udacity-dend/"

output_data = os.path.join(os.getcwd(), "data")
# output_data = "s3a://<url>/data/"


# # 1. load song data
import_source_category = 1

# 1.1a: local data source
if import_source_category == 1:
    input_location = os.path.join(input_data, "song-data", "*", "*", "*", "*.json")


#df_songs.write.csv(os.getcwd() + ("/data/songs2.csv"), sep=";")
#df_songs.write.mode("overwrite").csv(os.getcwd() + ("/data/songs.csv"))
#df_songs.write.mode("overwrite").parquet(os.getcwd() + ("/data/songs.parquet"))

# 1.2: S3 data source, single resource path
if import_source_category == 2:
    input_location = os.path.join(input_data, "song-data", "*", "*", "*", "*.json")
#df_songs = spark.read.schema(SONG_SCHEMA).json(input_location, multiLine="false")


# 1.3: S3 data source, multiple resource paths
if import_source_category == 3:
    fs = s3fs.S3FileSystem(anon=True)
    fs.ls(input_data)
    list_of_resources = ["s3a://" + elem for elem in fs.find("udacity-dend/song_data/", maxdepth=None, withdirs=True, detail=False)
                          if ".json" in elem]
    list_of_resources[:5]
    len(list_of_resources)

    input_location = list_of_resources

# 1 load data
df_songs = spark.read.schema(SONG_SCHEMA).json(input_location, multiLine="false")
df_songs.show(10, truncate=False)
logger.debug("Imported Song Data")

# persist song-data in parquet
output_location = os.path.join(output_data, "song-data.parquet")
df_songs.write.mode("overwrite").parquet(output_location)

# extract columns to create songs-table
songs_table = df_songs.select(["song_id", "title", "artist_id", "year", "duration"])
    
# persist songs-table to parquet (partitioned by year and artist)
output_location = os.path.join(output_data, "songs_table.parquet")
songs_table.write.mode('overwrite').partitionBy("year","artist_id").parquet(output_location)

# extract columns to create artists table
artists_table = df_songs.select(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"])
    
# persist artists-table to parquet
output_location = os.path.join(output_data,"artists_table.parquet")
artists_table.write.mode('overwrite').parquet(output_location)




# # 2. Log data

# Load json applying schema
#input_data = "s3a://udacity-dend/"
input_location = os.path.join(input_data,"log-data","*.json")
df_logs = spark.read.schema(LOG_SCHEMA).json(input_location, multiLine="true")
df_logs = df_logs.withColumn("start_time", F.from_unixtime(F.col("ts") / 1000))
df_logs.show(10, truncate=False)

output_location = os.path.join(output_data,"log-data.parquet")
print(output_location)
df_logs.write.mode("overwrite").parquet(output_location)


# ## users table
# filter by actions for song plays
df_logs = df_logs.where(F.col("page") == "NextSong")

# extract columns for users table, need to rename
users_table = df_logs.select(F.col("userId").alias("user_id"),
    F.col("firstName").alias("first_name"),
    F.col("lastName").alias("last_name"),
    F.col("gender"),
    F.col("level")
)

# persist users table to parquet
output_location = os.path.join(output_data,"users_table.parquet")
users_table.write.mode("overwrite").parquet(output_location)


# ## time_table

# extract columns to create time/table
time_table = df_logs \
    .withColumn("hour", F.hour("start_time"))\
    .withColumn("day", F.dayofmonth("start_time"))\
    .withColumn("weekday", F.dayofweek("start_time"))\
    .withColumn("month", F.month("start_time"))\
    .withColumn("week", F.weekofyear("start_time"))\
    .withColumn("year", F.year("start_time"))\
    .select(["start_time","hour","day","weekday","month","week","year"])

# write time table to parquet files partitioned by year and month
output_location = os.path.join(output_data,"time_table.parquet")
time_table.write.mode("overwrite").partitionBy("year","month").parquet(output_location)

# ## songs_played_info-table

# read in song data to use for songplays table
#input_location = os.path.join(input_data, "song-data", "*", "*", "*", "*.json")
#df_songs = spark.read.schema(SONG_SCHEMA).json(input_location, multiLine="false")
input_location = os.path.join(input_data, "song-data.parquet")
df_songs = spark.read.schema(SONG_SCHEMA).parquet(input_location)

# join log and song datasets to create songplays table
cond = [df_songs.title == df_logs.song, df_songs.artist_name == df_logs.artist]
songs_played_info = df_logs.join(df_songs, cond, "inner") \
    .select(
        F.monotonically_increasing_id().alias("songplay_id"),
        F.col("start_time"),
        F.col("userId").alias("user_id"),
        F.col("level"),
        F.col("song_id"),
        F.col("artist_id"),
        F.col("sessionId").alias("session_id"),
        F.col("location"),
        F.col("userAgent").alias("user_agent"),
        F.col("title"),
        F.col("song"),
        F.col("artist_name"),
        F.col("artist")
)
songs_played_info.collect()
songs_played_info.show(100)

"""
TABLE songplays(songplay_id serial, 
    start_time timestamp NOT NULL, 
    user_id int NOT NULL, 
    level varchar, 
    song_id varchar, 
    artist_id varchar, 
    session_id int NOT NULL, 
    location varchar, 
    user_agent varchar,
"""

df_logs.createOrReplaceTempView("log_table")
df_songs.createOrReplaceTempView("song_table")

songplay_query = """
SELECT monotonically_increasing_id() as songplay_id,
                                to_timestamp(lt.ts/1000) as start_time,
                                month(to_timestamp(lt.ts/1000)) as month,
                                year(to_timestamp(lt.ts/1000)) as year,
                                lt.userId as user_id,
                                lt.level as level,
                                st.song_id as song_id,
                                st.artist_id as artist_id,
                                lt.sessionId as session_id,
                                lt.location as location,
                                lt.userAgent as user_agent
                                FROM log_table lt
                                JOIN song_table st
                                 on (lt.artist = st.artist_name
                                  and lt.song = st.title)
"""

# extract columns from joined song and log datasets to create songplays table
songplays_table = spark.sql(songplay_query)
songplays_table.show()


# write songplays table to parquet files partitioned by year and month
file_name = "songplays_table.parquet"
output_location = output_data + file_name
songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_location)
