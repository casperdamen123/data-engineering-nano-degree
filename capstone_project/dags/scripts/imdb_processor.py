from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import logging
import os


def create_spark_session():
    """
    Function to generate Spark session
    """

    spark = (SparkSession
             .builder
             .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0")
             .getOrCreate()
             )

    return spark


def pre_process_crew_names(spark, s3_bucket, s3_project, s3_input_key):
    """
    Preparing the crew data belonging to movie titles

    Args:
        - spark (SparkSession): Spark session
        - s3_bucket (str): S3 bucket name
        - s3_input_key (str): S3 input data key
    Returns:
        - crew_enriched (SparkDataFrame): Enriched crew details
        - directors_mapping (SparkDataFrame): Mapping between director IDs and names
        - writers_mapping (SparkDataFrame): Mapping between writer IDs and names
    """

    df_crew = (spark
               .read.option("header", True).csv("s3://{}/{}/{}/crew.tsv".format(s3_bucket, s3_project, s3_input_key),
                                                sep=r'\t')
               .replace({"\\N": None})
               )

    df_names = (spark
                .read.option("header", True).csv("s3://{}/{}/{}/name_basics.tsv".format(s3_bucket,
                                                                                        s3_project,
                                                                                        s3_input_key),
                                                 sep=r'\t')
                .replace({"\\N": None})
                )

    get_length = F.udf(lambda value: 0 if value is None else len(value.split(',')), T.IntegerType())

    crew_enriched = (df_crew
                     .withColumn('hasDirectors', df_crew['directors'].isNotNull())
                     .withColumn('hasWriters', df_crew['writers'].isNotNull())
                     .withColumn('directors_arr', F.split(df_crew['directors'], ','))
                     .withColumn('writers_arr', F.split(df_crew['writers'], ','))
                     .withColumn('numDirectors', get_length(df_crew['directors']))
                     .withColumn('numWriters', get_length(df_crew['writers']))
                     )

    # Explode directors/writers array to multiple rows
    directors_exploded = crew_enriched.select("tconst", F.explode("directors_arr").alias("director_nconst"))
    writers_exploded = crew_enriched.select("tconst", F.explode("writers_arr").alias("writers_nconst"))

    # Join actual names on exploded dataframes
    directors_mapping = directors_exploded.join(df_names,
                                                directors_exploded["director_nconst"] == df_names['nconst'],
                                                how='left')
    writers_mapping = writers_exploded.join(df_names, writers_exploded["writers_nconst"] == df_names['nconst'],
                                            how='left')

    # Final mappers with actual names in array
    directors_mapping = (directors_mapping
                         .groupby('tconst')
                         .agg(F.concat_ws(", ", F.collect_list('primaryName'))
                              .alias("directorNames")
                              )
                         )

    writers_mapping = (writers_mapping
                       .groupby('tconst')
                       .agg(F.concat_ws(", ", F.collect_list('primaryName'))
                            .alias("writerNames")
                            )
                       )

    # Reduce the amount of directors/writers listed to 5
    reduce_names = F.udf(lambda value: ','.join(value.split(',')[:5]))
    directors_mapping_final = directors_mapping.withColumn('directorNames',
                                                           reduce_names(directors_mapping['directorNames']))
    writers_mapping_final = writers_mapping.withColumn('writerNames', reduce_names(writers_mapping['writerNames']))

    return crew_enriched, directors_mapping_final, writers_mapping_final


def pre_process_titles(spark, s3_bucket, s3_project, s3_input_key):
    """
    Preparing the movie title data

    Args:
        - spark (SparkSession): Spark session
        - s3_bucket (str): S3 bucket name
        - s3_input_key (str): S3 input data key

    Returns:
        - titles_enriched (Spark DataFrame): Enriched movie title details
    """

    df_titles = (spark
                 .read.option("header", True).csv("s3://{}/{}/{}/title_basics.tsv".format(s3_bucket, s3_project,
                                                                                          s3_input_key),
                                                  sep=r'\t')
                 .replace({"\\N": None})
                 )

    has_ended = F.udf(lambda end_year: False if end_year is None else True, T.BooleanType())

    get_length = F.udf(lambda value: 0 if value is None else len(value.split(',')), T.IntegerType())

    titles_enriched = (df_titles
                       .withColumn('isTvSeries', df_titles['titleType'] == "tvEpisode")
                       .withColumn('isAdult', df_titles['isAdult'] == "1")
                       .withColumn('numOfGenres', get_length(df_titles['genres']))
                       .withColumn('releaseYear', df_titles['startYear'])
                       .withColumn('hasEnded', has_ended(df_titles['endYear']))
                       )

    return titles_enriched


def pre_process_ratings(spark, s3_bucket, s3_project, s3_input_key):
    """
    Preparing the movie ratings data

    Args:
        - spark (SparkSession): Spark session
        - s3_bucket (str): S3 bucket name
        - s3_input_key (str): S3 input data key
    Returns:
        - ratings (Spark DataFrame): Enriched movie title details
    """

    ratings = (spark
               .read.option("header", True).csv("s3://{}/{}/{}/ratings.tsv".format(s3_bucket, s3_project, s3_input_key),
                                                sep=r'\t')
               .replace({"\\N": None})
               )

    return ratings


def join_prepared_data(titles_df, crew_df, directors_mapping, writers_mapping,
                       ratings_df, s3_bucket, s3_project, s3_output_key):
    """
    Args:
        titles_df (SparkSession.DataFrame): Pre-processed dataframe containing movie title details
        crew_df (SparkSession.DataFrame): Pre-processed dataframe containing crew details
        directors_mapping (SparkSession.DataFrame): Pre-processed dataframe containing director id/names mapping
        writers_mapping (SparkSession.DataFrame): Pre-processed dataframe containing writer id/names mapping
        ratings_df (SparkSession.DataFrame): Pre-processed dataframe containing movie ratings
        s3_bucket (str): S3 bucket name
        s3_project (str): Project folder key
        s3_output_key (str): Output folder key

    Returns:
        final_path (str): S3 destination path for final table

    """

    final_table = (titles_df
                   .join(crew_df, on='tconst', how='left')
                   .join(directors_mapping, on='tconst', how='left')
                   .join(writers_mapping, on='tconst', how='left')
                   .join(ratings_df, on='tconst', how='left')
                   .selectExpr('tconst as title_key',
                               'substring(primaryTitle, 1, 200) AS primary_title',
                               'substring(originalTitle, 1, 200) AS original_title',
                               'titleType AS title_type',
                               'directorNames AS directors',
                               'writerNames AS writers',
                               'genres',
                               'hasDirectors AS has_directors',
                               'hasWriters AS has_writers',
                               'hasEnded AS has_ended',
                               'numOfGenres AS num_of_genres',
                               'CAST(releaseYear AS INT) AS release_year',
                               'CAST(endYear AS INT) AS end_year',
                               'CAST(averageRating AS FLOAT) AS average_rating',
                               'CAST(numVotes AS INT) AS num_votes',
                               'CAST(runtimeMinutes AS INT) AS runtime_minutes',
                               'numDirectors AS num_directors',
                               'numWriters AS num_writers')
                   )

    final_path = "s3://{}/{}/{}/imdb_movie_details".format(s3_bucket, s3_project, s3_output_key)

    logging.info(f"Final table schema: \n {final_table.printSchema()}")
    final_table.write.mode("overwrite").parquet(final_path)

    return final_path


def main():
    """
    General process that invokes the relevant data processing functions
    """

    spark = create_spark_session()

    # Set S3 info from environment variables
    s3_bucket = os.environ['S3_BUCKET']
    s3_project = os.environ['S3_PROJECT']
    s3_input_key = os.environ['S3_INPUT_KEY']
    s3_output_key = os.environ['S3_OUTPUT_KEY']

    # Prepare different tables
    logging.info("Preparing crew names data")
    crew_df, directors_mapping, writers_mapping = pre_process_crew_names(spark, s3_bucket, s3_project, s3_input_key)
    logging.info("Preparing movie title data")
    titles_df = pre_process_titles(spark, s3_bucket, s3_project, s3_input_key)
    logging.info("Preparing ratings data")
    ratings_df = pre_process_ratings(spark, s3_bucket, s3_project, s3_input_key)

    # Create final table
    logging.info("Merging prepared data")
    final_table = join_prepared_data(titles_df, crew_df, directors_mapping, writers_mapping,
                                     ratings_df, s3_bucket, s3_project, s3_output_key)
    logging.info("Final table pushed to {}".format(final_table))


if __name__ == '__main__':
    main()
