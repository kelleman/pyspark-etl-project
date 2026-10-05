from pyspark.sql import DataFrame


def load_to_postgresql(df: DataFrame):
    """
    Loads the transformed dataframe into PostgreSQL.
    """

    print("\n========== LOADING DATA TO POSTGRESQL ==========\n")

    (
        df.write
        .format("jdbc")
        .option(
            "url",
            "jdbc:postgresql://localhost:5432/ufo_db"
        )
        .option(
            "dbtable",
            "ufo_sightings"
        )
        .option(
            "user",
            "postgres"
        )
        .option(
            "password",
            "postgres"
        )
        .option(
            "driver",
            "org.postgresql.Driver"
        )
        .mode("overwrite")
        .save()
    )

    print("Data successfully loaded into PostgreSQL.")