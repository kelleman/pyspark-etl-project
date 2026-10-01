from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "Spark ETL Project"):
    """
    Creates and returns a Spark Session.
    """

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    return spark

