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

############ Vorbereitung zum Lesen der JSON (aus S3a)
#df_logs = spark1.read.json("s3a://udacity-dend/log_data")
#df_songs = spark1.read.json("s3a://udacity-dend/song_data")

log_path = Path().cwd().joinpath("data/log-data").as_uri()
# df = spark1.read.json(log_path)

# Create schema for jsons of log
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

# Load json applying log schema
df_log = spark1.read.schema(LOG_SCHEMA).json(log_path)
df_log.show(10, truncate=False)
