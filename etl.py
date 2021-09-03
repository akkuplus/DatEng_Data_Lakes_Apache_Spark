import configparser
from datetime import datetime
import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear\
    , date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

#os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']



def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    if False:
        song_data = os.path.join(input_data.as_uri(),"song-data", "*", "*", "*", "*.json")
    else:
        song_data = input_data.joinpath("song-data.zip")

    # read song data
    df = spark.read.schema(SONG_SCHEMA).json(song_data, multiLine="false")

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"])
    
    # write songs table to parquet files partitioned by year and artist
    file_name = "songs_table.parquet"
    output_path = output_data.joinpath("song-data").joinpath(file_name)
    songs_table.write.partitionBy("year","artist_id").parquet(output_path)


    # extract columns to create artists table
    artists_table = df.select(["artist_id","artist_name","artist_location","artist_latitude", "artist_longitude"])
    
    # write artists table to parquet files
    file_name = "artists_table.parquet"
    output_path = Path().cwd().joinpath("data").joinpath("song-data").joinpath(file_name).as_uri()
    artists_table.write.parquet(output_path)


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    if False:
        log_data = os.path.join(input_data.as_uri(),"log-data", "*.json")
    else:
        log_data = input_data.joinpath("log-data.zip")

    # read log data file
    df = spark.read.schema(LOG_SCHEMA).json(log_data, multiLine="true")
    
    # filter by actions for song plays
    df = df.where(col("page") == "NextSong")

    # extract columns for users table, need to rename
    users_table = df.select(col("userId").alias("user_id"),
        col("firstName").alias("first_name"),
        col("lastName").alias("last_name"),
        "gender",
        "level"
    )

    # write users table to parquet files
    file_name = "users_table.parquet"
    output_path = output_data.joinpath("log-data").joinpath(file_name)
    users_table.write.parquet(output_path)

    # prepare time table - create timestamp column from original timestamp column via udf
    get_datetimestamp = udf(lambda x: datetime.utcfromtimestamp(x / 1000), TimestampType())
    df = df.withColumn("start_time", get_datetimestamp("ts"))

    # extract columns to create time table
    time_table = df.withColumn("hour", hour("start_time")) \
        .withColumn("day", dayofmonth("start_time")) \
        .withColumn("weekday", dayofweek("start_time")) \
        .withColumn("month", month("start_time")) \
        .withColumn("week", weekofyear("start_time")) \
        .withColumn("year", year("start_time")) \
        .select(["start_time","hour","day","week","weekday","month","year"])
    
    # write time table to parquet files partitioned by year and month
    file_name = "time_table.parquet"
    output_path = output_data.joinpath("log-data").joinpath(file_name)
    time_table.write.partitionBy("year","month").parquet(output_path)

    # read in song data to use for songplays table - songs and artists table have already saved as parquet, reload!
    output_data = Path().cwd().joinpath("data").joinpath("song-data")

    file_name = "artists_table.parquet"
    artists_df = spark.read.parquet(os.path.join(output_data, output_data.joinpath(file_name)))

    file_name = "songs_table.parquet"
    songs_df = spark.read.parquet(os.path.join(output_data, output_data.joinpath(file_name)))

    # join artists and songs
    songs_played_info = songs_df.join(artists_df, "artist_id", "inner").select(
        "song_id",
        "title",
        "artist_id",
        col("name").alias("artist_name"),
        "duration"
    )

    # extract columns from joined songs_played_info and log datasets to create songplays table
    songplays_table = songs_played_info.select(
        monotonically_increasing_id().alias("songplay_id"),
        "start_time",
        col("userId").alias("user_id"),
        "level",
        "song_id",
        "artist_id",
        col("sessionId").alias("session_id"),
        "location",
        col("userAgent").alias("user_agent"),
        month("start_time").alias("month"),
        year("start_time").alias("year")
    )

    # write songplays table to parquet files partitioned by year and month
    file_name = "songplays_table.parquet"
    output_path = output_data.joinpath("log-data").joinpath(file_name)
    songplays_table.write.partitionBy("year", "month").parquet(output_path)


def main():
    spark = create_spark_session()
    #input_data = "s3a://udacity-dend/"
    input_data = os.path.join(os.getcwd(), "data")
    output_data = "" or Path().cwd().joinpath("data")
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
