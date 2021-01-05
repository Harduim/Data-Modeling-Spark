from datetime import datetime
import os
from json import loads
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

with open("dl.json") as js:
    config = loads(js.read())

os.environ["AWS_ACCESS_KEY_ID"] = config["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["AWS_SECRET_ACCESS_KEY"]


def create_spark_session():
    spark = SparkSession.builder.config(
        "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    ).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def process_song_data(spark, input_data, output_data, only_sample=True):
    song_path = "song_data/*/*/*/*.json" if not only_sample else "song_data/A/A/A/*.json"
    song_data_path = os.path.join(input_data, song_path)
    song_data = spark.read.json(song_data_path).dropDuplicates().cache()
    song_data.createOrReplaceTempView("song_data_raw")

    songs_table = spark.sql(
        "SELECT DISTINCT(song_id) as song_id, title, artist_id, year, duration FROM song_data_raw;"
    )
    songs_table.createOrReplaceTempView("song_data")

    # Extracting columns to create the artists table
    artists_table = spark.sql(
        """SELECT DISTINCT(artist_id),
                  artist_name,
                  artist_location,
                  artist_latitude,
                  artist_longitude
            FROM song_data_raw;"""
    )
    artists_table.createOrReplaceTempView("artists")

    # Leaving write for last in case anything goes wrong in the conversion step
    # Writing artists table to parquet files
    songs_output = os.path.join(output_data, "artists_parquet")
    artists_table.write.parquet(songs_output, "overwrite")

    # Writing songs table to parquet files partitioned by year and artist
    songs_output = os.path.join(output_data, "songs_parquet")
    songs_table.write.partitionBy("year", "artist_id").parquet(songs_output, "overwrite")


def process_log_data(spark, input_data, output_data):
    events_path = os.path.join(input_data, "log_data/2018/11/*events.json")
    log_data = spark.read.json(events_path).dropDuplicates()
    log_data.createOrReplaceTempView("events_raw")

    events = spark.sql("SELECT * FROM events_raw WHERE page = 'NextSong';").cache()
    events.createOrReplaceTempView("events_raw")

    user_table = spark.sql(
        """SELECT DISTINCT(userId) as user_id,
                firstName,
                lastName,
                gender,
                level
            FROM events_raw;"""
    )

    songplays_table = spark.sql(
        """SELECT unixtime(evnt.ts / 1000) as start_time,
                evnt.userId as user_id
                evnt.level,
                songs.song_id,
                artists.artist_id,
                evnt.sessionId as session_id,
                evnt.location,
                evnt.user_agent
            FROM events_raw AS evnt
            INNER JOIN artists on evnt.artist = artists.artist_name
            INNER JOIN songs ON evnt.song = songs.title AND evnt.lengh = song.duration
            """
    )

    # songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

    time_table = spark.sql(
        """SELECT row_number() as songplay_id
                  start_time,
                  hour(start_time) as hour,
                  day(start_time) as day,
                  weekofyear(start_time) as week,
                  month(start_time) as month,
                  year(start_time) as year,
                  dayofweek(start_time) as weekday
            FROM songplays;"""
    )

    # Leaving write for last in case anything goes wrong in the conversion step
    time_output = os.path.join(output_data, "time_parquet")
    time_table.write.parquet(time_output, "overwrite")

    user_output = os.path.join(output_data, "users_parquet")
    user_table.write.parquet(user_output, "overwrite")

    songplays_ouput = os.path.join(output_data, "songplays_parquet")
    songplays_table.write.parquet(songplays_ouput, "overwrite")


if __name__ == "__main__":
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = config["OUTPUT_BUCKET"]

    # process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)