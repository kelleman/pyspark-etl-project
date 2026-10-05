from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "Spark ETL Project"):
    """
    Creates and returns a Spark Session.
    """

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.7.8"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    return spark