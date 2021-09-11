import configparser
from datetime import datetime
import logging as Log
import os
from pathlib import Path
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

logger = Log.getLogger(__name__)
logger.setLevel(Log.DEBUG)


def create_spark_session():
    """
    Create or get a Spark session.

    :return: SparkSession
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    logger.debug("Created Data Schemas")

    spark._jsc.hadoopConfiguration()\
        .set("fs.s3a.access.key", os.environ['AWS_ACCESS_KEY_ID'])
    spark._jsc.hadoopConfiguration()\
        .set("fs.s3a.secret.key", os.environ['AWS_SECRET_ACCESS_KEY'])

    return spark


def get_schemas(name: str):
    """
    Return structure data / schema applied to the data.

    :param name: name if requested schema: SONG or LOG
    :return: requested schema.
    """

    if name.upper() == 'LOG':
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

        return LOG_SCHEMA

    if name.upper() == 'SONG':
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

        return SONG_SCHEMA


def process_song_data(spark, input_data, output_data):
    """
    Process the song data files in "song-data" subdirectory
     and derive songs-table and artists-table.

    :param spark: SparkSession
    :param input_data: Project path for Input data. Assuming subdirectory "song-data" holds song data within jsons files.
    :param output_data: Project path for Output data.
    """

    # get filepath to song data file
    input_location = os.path.join(input_data, "song-data", "*", "*", "*", "*.json")

    # load song data
    SONG_SCHEMA = get_schemas('SONG')
    df_songs = spark.read.schema(SONG_SCHEMA).json(input_location, multiLine="false")
    df_songs.show(10, truncate=False)
    logger.debug("Imported Song Data")

    # store song data to parquet
    output_location = os.path.join(output_data, "song-data.parquet")
    df_songs.write.mode("overwrite").parquet(output_location)
    logger.debug("Exported Song Data to parquet")

    # songs-table: extract columns to create songs-table
    songs_table = df_songs.select(["song_id", "title", "artist_id", "year", "duration"])
    
    # songs-table: persist to parquet (partitioned by year and artist)
    output_location = os.path.join(output_data, "songs_table.parquet")
    songs_table.write.mode('overwrite').partitionBy("year","artist_id").parquet(output_location)
    logger.debug("Exported songs table as parquet")

    # artists-table: extract columns to create artists-table
    artists_table = df_songs.select(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"])

    # artists-table: persist artists-table to parquet
    output_location = os.path.join(output_data, "artists_table.parquet")
    artists_table.write.mode('overwrite').parquet(output_location)
    logger.debug("Exported artists table as parquet")

    return


def process_log_data(spark, input_data, output_data):
    """
    Process the log data files in "log-data" subdirectory.
    Therewith derive users-table, time-table, and songplays-table.

    :param spark: SparkSession
    :param input_data: Project path for Input data. Assuming subdirectory "song-data" holds song data within jsons files.
    :param output_data: Project path for Ouput data.
    """

    # set data locations
    input_location = os.path.join(input_data, "log-data", "*.json")

    # read log data file
    LOG_SCHEMA = get_schemas('LOG')
    df_logs = spark.read.schema(LOG_SCHEMA).json(input_location, multiLine="true")
    logger.debug("Imported Log Data")

    # filter by actions for song plays
    df_logs = df_logs.where(F.col("page") == "NextSong")

    # transform timestamp
    df_logs = df_logs.withColumn("start_time", F.from_unixtime(F.col("ts") / 1000))
    df_logs.show(10, truncate=False)
    logger.debug("Transformed Log Data")

    # persist log-data
    output_location = os.path.join(output_data, "log-data.parquet")
    df_logs.write.mode("overwrite").parquet(output_location)
    logger.debug("Exported log data as parquet")

    # users table: extract columns, need to rename
    users_table = df_logs.select(
        F.col("userId").alias("user_id"),
        F.col("firstName").alias("first_name"),
        F.col("lastName").alias("last_name"),
        F.col("gender"),
        F.col("level")
    )

    # users-table: write to parquet files
    output_location = os.path.join(output_data, "users_table.parquet")
    users_table.write.mode("overwrite").parquet(output_location)
    logger.debug("Exported users table as parquet")

    # time-table: extract columns to create
    time_table = df_logs.withColumn("hour", F.hour("start_time")) \
        .withColumn("day", F.dayofmonth("start_time")) \
        .withColumn("weekday", F.dayofweek("start_time")) \
        .withColumn("month", F.month("start_time")) \
        .withColumn("week", F.weekofyear("start_time")) \
        .withColumn("year", F.year("start_time")) \
        .select(["start_time", "hour", "day", "week", "weekday", "month", "year"])
    
    # time-table: write to parquet files partitioned by year and month
    output_location = os.path.join(output_data, "time_table.parquet")
    time_table.write.mode("overwrite").partitionBy("year","month").parquet(output_location)
    logger.debug("Exported time table as parquet")

    # songplays-table: read in song data to use for
    output_location = os.path.join(output_data,"song-data.parquet")
    df_songs = spark.read.parquet(output_location)
    logger.debug("Imported song data from parquet")

    # songplays-table: join datasets of logs and songs to create the new table
    cond = [df_songs.title == df_logs.song, df_songs.artist_name == df_logs.artist]
    songplays_table = df_logs.join(df_songs, cond, "inner") \
        .withColumn("year", F.year("start_time")) \
        .withColumn("month", F.month("start_time")) \
        .select(
            F.monotonically_increasing_id().alias("songplay_id"),
            F.col("start_time"),
            F.col("year"),
            F.col("month"),
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
    songplays_table.collect()
    songplays_table.show(100)
    logger.debug("Derivered songplays data")

    # songplays-table: write to parquet files partitioned by year and month
    output_location = os.path.join(output_data, "songplays_table.parquet")
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_location)
    logger.debug("Exported songplays table as parquet")

    return


def main():
    """
    Implement main ETL functionality.
    """

    spark = create_spark_session()
    logger.debug("Created Spark session")

    input_data = os.path.join(os.getcwd(), "data")  # Alternative: input_data = "s3a://udacity-dend/"
    logger.debug(f"Set input_data to {input_data}")

    output_data = os.path.join(os.getcwd(), "data")  # Alternative: output_data = "s3a://<url>/data/"
    logger.debug(f"Set output_data to {output_data}")

    process_song_data(spark, input_data, output_data)
    logger.debug(f"Processed song data and derive songs and artists table")

    process_log_data(spark, input_data, output_data)
    logger.debug(f"Processed log data and derive users, time and songplays table")

    return


if __name__ == "__main__":
    main()
