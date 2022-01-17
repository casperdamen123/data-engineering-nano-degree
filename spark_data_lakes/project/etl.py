import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

config = configparser.ConfigParser()
config.read('aws.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    Function to generate Spark session
    """

    spark = (SparkSession
             .builder
             .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")
             .getOrCreate()
            )

    return spark

def process_song_data(spark, song_input_data, output_data):
    """
    Retrieve raw song data files and transform to generate:
    - Songs table
    - Artists table
    - Songplay table
    """

    # Load all song files to single Spark dataframe
    df_song = spark.read.json(song_input_data)

    # Extract columns to create songs table
    song_table_cols = ['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_table = df_song.selectExpr(*song_table_cols).distinct()

    # Write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data + 'songs.parquet')

    # Extract columns to create artists table
    artists_table_cols = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artists_table = df_song.selectExpr(*artists_table_cols).distinct()

    # Write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists.parquet')


def process_log_data(spark, log_input_data, song_input_data, output_data):
    """
    Retrieve raw event data files and transform to generate:
    - Users table
    - Time table
    -
    """

    # Read log data files
    df_log = spark.read.json(log_input_data)

    # Filter by actions for song plays
    df_log = df_log.filter(df_log['Page'] == 'NextSong')

    # Extract columns for users table
    users_table_cols = [
        'userId AS user_id',
        'firstName AS first_name',
        'lastName AS last_name',
        'gender',
        'level'
    ]

    users_table = df_log.selectExpr(*users_table_cols).distinct()

    # Write users table to parquet files
    users_table.write.parquet(output_data + 'users.parquet')

    # Create datetime column from original timestamp column
    df_log = df_log.withColumn('datetime', F.from_unixtime(df_log['ts'] / 1000))

    # Extract columns to create time table
    time_table_cols = [
        'datetime AS start_time',
        'hour(datetime) AS hour',
        'day(datetime) AS day',
        'weekofyear(datetime) AS week',
        'dayofweek(datetime) AS weekday',
        'month(datetime) AS month',
        'year(datetime) AS year',
    ]

    time_table = df_log.selectExpr(*time_table_cols).distinct()

    # Write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(output_data + 'time.parquet')

    # Read in song data to use for songplays table
    song_df = spark.read.json(song_input_data)

    # Extract columns from joined song and log datasets to create songplays table
    songplay_table_cols = [
        'datetime AS start_time',
        'month(datetime) AS month',
        'year(datetime) AS year',
        'userId AS user_id',
        'level',
        'song_id',
        'artist_id',
        'sessionId AS session_id',
        'location',
        'userAgent AS user_agent'
    ]

    songplay_table = song_df.join(df_log, song_df['title'] == df_log['song']).selectExpr(
        *songplay_table_cols).distinct()

    # Write songplays table to parquet files partitioned by year and month
    songplay_table.write.partitionBy('year', 'month').parquet(output_data + 'songplays.parquet')


def main():
    """
    General process that invokes the relevant data processing functions
    """

    spark = create_spark_session()

    log_input_data = "s3://udacity-data-nano-degree-de/log_data/*/*/*.json"
    song_input_data = "s3://udacity-data-nano-degree-de/song_data/*/*/*/*.json"
    output_data = "s3://udacity-data-nano-degree-de/sparkify/"

    process_song_data(spark, song_input_data, output_data)
    process_log_data(spark, log_input_data, song_input_data, output_data)

if __name__ == "__main__":
    main()