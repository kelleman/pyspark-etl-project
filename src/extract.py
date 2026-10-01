from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


def read_csv(
    spark: SparkSession,
    file_path: str,
    header: bool = True,
    infer_schema: bool = True,
) -> DataFrame:
    """
    Reads a CSV file and returns a Spark DataFrame.
    """

    df = (
        spark.read
        .option("header", header)
        .option("inferSchema", infer_schema)
        .csv(file_path)
    )

    return df