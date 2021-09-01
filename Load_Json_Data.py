import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

spark1 = SparkSession.builder\
                    .master('local[*]')\
                    .getOrCreate()

# Preparation Reading JSON from S3a
#df_logs = spark1.read.json("s3a://udacity-dend/log_data")
#df_songs = spark1.read.json("s3a://udacity-dend/song_data")

log_path = Path().cwd().joinpath("data/log-data").as_uri()
# df = spark1.read.json(log_path)

# Create schema for jsons of logs
LOG_SCHEMA = StructType([
    StructField("artist", StringType(), False),
    StructField("auth", StringType()),
    StructField("firstName", StringType()),
    StructField("gender", StringType()),
    StructField("itemInSession", LongType()),
    StructField("lastName", StringType()),
    StructField("length", DoubleType()),
    StructField("level", StringType()),
    StructField("location", StringType()),
    StructField("method", StringType()),
    StructField("page", StringType()),
    StructField("registration", DoubleType()),
    StructField("sessionId", LongType()),
    StructField("song", StringType()),
    StructField("status", LongType()),
    StructField("ts", LongType()),
    StructField("userAgent", StringType()),
])

# Load json applying song schema
df_log = spark1.read.schema(LOG_SCHEMA).json(log_path)
df_log.show(10, truncate=False)

# Preparation song data
song_path = Path().cwd().joinpath("data").joinpath("song-data").joinpath("*").joinpath("*").joinpath("*").joinpath("*.json").as_uri()
song_path = os.path.join(os.getcwd(), "data", "song-data", "*", "*", "*", "*.json")
# df = spark1.read.json(log_path)

# Create schema for jsons of songs
SONG_SCHEMA = StructType([
    StructField("artist_id", StringType()),
    StructField("artist_latitude", DoubleType()),
    StructField("artist_location", StringType()),
    StructField("artist_longitude", DoubleType()),
    StructField("artist_name", StringType()),
    StructField("duration", DoubleType()),
    StructField("num_songs", IntegerType()),
    StructField("song_id", StringType()),
    StructField("title", StringType()),
    StructField("year", IntegerType()),
])

# Load json applying log schema
df_log = spark1.read.schema(SONG_SCHEMA).json(song_path, multiLine="false")
#
df_log.show(10, truncate=False)
