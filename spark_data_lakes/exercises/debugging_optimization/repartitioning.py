from pyspark.sql import SparkSession

def repartition(input_path: str, output_name: str, initial_partition_column: str, number_of_workers: int):
    
    # Initiate Spark session
    spark = SparkSession.builder.appName("Repartition").getOrCreate()

    # Load data
    df = spark.read.format(input_path, header=True).load()

    # Partition by start_year
    df.write.partitionBy(initial_partition_column).csv(output_name)

    # Re-partition by number of workers
    df.repartition(number_of_workers).write.csv("file")

if __name__ == "__main__":
    repartition('<INPUT VALUES>')