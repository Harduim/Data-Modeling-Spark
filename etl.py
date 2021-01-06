import os
from json import loads

from pyspark.sql import SparkSession

with open("dl.json") as js:
    config = loads(js.read())

os.environ["AWS_ACCESS_KEY_ID"] = config["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["AWS_SECRET_ACCESS_KEY"]


def create_spark_session() -> SparkSession:
    """Creates a spark session

    Returns:
        sparkSession
    """
    spark = SparkSession.builder.config(
        "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    ).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def process_song_data(spark: SparkSession, input_data: str, output_data: str):
    """Process the song files

    Args:
        spark (SparkSession): Spark Session
        input_data (str): input data path
        output_data (str): output data path
    """
    song_data_path = os.path.join(input_data, "song_data/*/*/*/*.json")
    song_data = spark.read.json(song_data_path).dropDuplicates().cache()
    song_data.createOrReplaceTempView("song_data_raw")

    songs_table = spark.sql(
        "SELECT DISTINCT(song_id) as song_id, title, artist_id, year, duration FROM song_data_raw;"
    )
    songs_table.createOrReplaceTempView("songs")

    # Extracting columns to create the artists table
    artists_table = spark.sql(
        """SELECT DISTINCT(artist_id),
                  artist_name as name,
                  artist_location as location,
                  artist_latitude as latitude,
                  artist_longitude as longitude
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


def process_log_data(spark: SparkSession, input_data: str, output_data: str):
    """Process events

    Args:
        spark (SparkSession): Spark Session
        input_data (str): input data path
        output_data (str): output data path
    """
    events_path = os.path.join(input_data, "log_data/2018/11/*events.json")
    log_data = spark.read.json(events_path).dropDuplicates()
    log_data.createOrReplaceTempView("events_raw")

    events = spark.sql("SELECT * FROM events_raw WHERE page = 'NextSong';").cache()
    events.createOrReplaceTempView("events_raw")

    user_table = spark.sql(
        """SELECT DISTINCT(userId) as user_id,
                firstName as first_name,
                lastName as last_name,
                gender,
                level
            FROM events_raw;"""
    )

    songplays_table = spark.sql(
        """SELECT uuid() as songplay_id,
                from_unixtime(ts / 1000) as start_time,
                userId as user_id,
                level,
                songs.song_id,
                artists.artist_id,
                sessionId as session_id,
                events_raw.location,
                userAgent
            FROM events_raw
            JOIN artists on events_raw.artist = artists.name
            JOIN songs on events_raw.song = songs.title"""
    )
    songplays_table.createOrReplaceTempView("songplays")

    time_table = spark.sql(
        """SELECT DISTINCT(start_time),
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
    time_table.write.partitionBy("year", "month").parquet(time_output, "overwrite")

    user_output = os.path.join(output_data, "users_parquet")
    user_table.write.parquet(user_output, "overwrite")

    songplays_ouput = os.path.join(output_data, "songplays_parquet")
    songplays_table.write.parquet(songplays_ouput, "overwrite")


if __name__ == "__main__":
    spark = create_spark_session()
    input_data = config["INPUT_BUCKET"]
    output_data = config["OUTPUT_BUCKET"]

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)