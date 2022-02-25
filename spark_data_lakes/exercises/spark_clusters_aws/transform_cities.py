from pyspark.sql import SparkSession
import pyspark.sql.functions as F

if __name__ == "__main__":
    spark = (SparkSession
             .builder
             .appName("wrangling-data")
             .getOrCreate()
             )

    # Set cities path
    cities_path = "s3://udacity-data-nano-degree-de/cities.csv"

    # Import as Spark dataframe
    cities = spark.read.csv(cities_path, header=True)

    # UDF's to transform coordinates
    get_latitude = F.udf(lambda coordinates: coordinates.split(' ')[0])
    get_longitude = F.udf(lambda coordinates: coordinates.split(' ')[1])

    # Clean and transform coordinates
    transformed_cities = cities.withColumn('coordinates', F.regexp_extract(cities['coords'], '(?<=\().+?(?=\))', 0))
    transformed_cities = (transformed_cities
                          .withColumn('latitude', get_latitude(transformed_cities['coordinates']))
                          .withColumn('longitude', get_longitude(transformed_cities['coordinates']))
                          )

    # Drop old coordinates
    transformed_cities = transformed_cities.drop(*['coords', 'coordinates'])

    # Save to S3
    transformed_cities.write.csv(path="s3a://udacity-data-nano-degree-de/cities", header=True)
