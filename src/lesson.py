from pyspark.sql import SparkSession


def create_spark_session(myApp: str = "ETL Project"):


	spark = (
		SparkSession.builder
		.appName(myApp)
		.machine("local[*]")
		.getOrCreate()
		)
	spart.sparkContext.setLogLevel("ERROR")
	return spark