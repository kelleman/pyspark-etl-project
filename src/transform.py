from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ==========================================================
# DATA INSPECTION FUNCTIONS
# ==========================================================

def display_schema(df: DataFrame):
    """
    Displays the dataframe schema.
    """

    print("\n========== SCHEMA ==========\n")

    df.printSchema()


def display_basic_information(df: DataFrame):
    """
    Displays basic information about the dataframe.
    """

    print("\n========== BASIC INFORMATION ==========\n")

    print(f"Rows: {df.count()}")
    print(f"Columns: {len(df.columns)}")

    print("\nColumn Names:")

    for column in df.columns:
        print(f"- {column}")

    print("\nData Types:")

    for dtype in df.dtypes:
        print(dtype)


def preview_data(df: DataFrame, rows: int = 10):
    """
    Displays sample rows from the dataframe.
    """

    print("\n========== DATA PREVIEW ==========\n")

    df.show(rows, truncate=False)


def descriptive_statistics(df: DataFrame):
    """
    Displays descriptive statistics.
    """

    print("\n========== SUMMARY ==========\n")

    df.describe().show()


# ==========================================================
# DATA CLEANING FUNCTIONS
# ==========================================================

def display_null_values(df: DataFrame):
    """
    Displays the number of NULL values in each column.
    """

    print("\n========== NULL VALUES ==========\n")

    null_counts = df.select([
        F.count(
            F.when(F.col(column).isNull(), column)
        ).alias(column)
        for column in df.columns
    ])

    null_counts.show(truncate=False)


def display_empty_values(df: DataFrame):
    """
    Displays the number of empty string values in each column.
    """

    print("\n========== EMPTY VALUES ==========\n")

    for column in df.columns:

        empty_count = df.filter(
            F.trim(F.col(column)) == ""
        ).count()

        print(f"{column}: {empty_count}")


def display_duplicates(df: DataFrame):
    """
    Displays the number of duplicate rows.
    """

    print("\n========== DUPLICATES ==========\n")

    total_rows = df.count()
    distinct_rows = df.distinct().count()

    duplicates = total_rows - distinct_rows

    print(f"Total rows: {total_rows}")
    print(f"Distinct rows: {distinct_rows}")
    print(f"Duplicate rows: {duplicates}")


def display_duplicate_rows(df: DataFrame, rows: int = 10):
    """
    Displays sample duplicate rows from the dataframe.

    Duplicate rows are identified by comparing all columns.
    """
    print("\n========== DUPLICATE ROWS ==========\n")

    duplicate_rows = (
        df.groupBy(df.columns)
        .count()
        .filter(F.col("count") > 1)
    )

    duplicate_rows.show(rows, truncate=False)


def remove_duplicate_rows(df: DataFrame) -> DataFrame:
    """
    Removes exact duplicate rows from the dataframe.

    Duplicate detection is based on all columns.
    """
    print("\n========== REMOVING DUPLICATES ==========\n")

    cleaned_df = df.dropDuplicates()

    return cleaned_df



def fill_null_values(df: DataFrame) -> DataFrame:
    """
    Replaces NULL values in selected columns with 'Unknown'
    and returns the cleaned dataframe.
    """

    print("\n========== FILLING NULL VALUES ==========\n")

    cleaned_df = df.fillna({
        "City": "UNKNOWN",
        "Colors Reported": "UNKNOWN",
        "Shape Reported": "UNKNOWN"
    })

    return cleaned_df


# ==========================================================
# DATA TRANSFORMATION FUNCTIONS
# ==========================================================

def standardize_column_names(df: DataFrame) -> DataFrame:
    """
    Standardizes dataframe column names by:
    - converting names to lowercase
    - replacing spaces with underscores
    """
    print("\n========== STANDARDIZING COLUMN NAMES ==========\n")

    standardized_df = df

    for column in df.columns:
        new_column = column.lower().replace(" ", "_")

        standardized_df = standardized_df.withColumnRenamed(
            column,
            new_column
        )

    return standardized_df


def convert_time_to_timestamp(df: DataFrame) -> DataFrame:
    """
    Converts the time column from string to timestamp.
    """
    print("\n========== CONVERTING TIME TO TIMESTAMP ==========\n")

    transformed_df = df.withColumn(
        "time",
        F.to_timestamp(
            F.col("time"),
            "M/d/yyyy H:mm"
        )
    )

    return transformed_df


def extract_time_information(df: DataFrame) -> DataFrame:
    """
    Extracts date, year, month, hour, day of the week, and month name, from the time column.
    """
    print("\n========== EXTRACTING TIME/DATE INFORMATION ==========\n")

    transformed_df = (
        df
        .withColumn("date", F.to_date("time"))
        .withColumn("year", F.year("time"))
        .withColumn("month", F.month("time"))
        .withColumn("hour", F.hour("time"))
        .withColumn("day_of_week", F.date_format("time", "EEEE"))
        .withColumn("month_name", F.date_format("time", "MMMM"))
    )

    return transformed_df

def count_reported_colors(df: DataFrame) -> DataFrame:
    """
    Counts the number of colors reported for each UFO sighting.
    """
    print("\n========== COUNTING REPORTED COLORS ==========\n")

    transformed_df = df.withColumn(
        "color_count",
        F.when(
            F.col("colors_reported") == "UNKNOWN",
            0
        ).otherwise(
            F.size(F.split(F.col("colors_reported"), " "))
        )
    )

    return transformed_df

# This is a temporaly function to inspect each column individually for necessary transformation or standardization
#The individual columns are specified in the main.py at the function call
def display_unique_values(df: DataFrame, column: str):
    """
    Displays the unique values in a specified column.
    """
    print(f"\n========== UNIQUE VALUES ({column}) ==========\n")

    df.select(column) \
      .distinct() \
      .orderBy(column) \
      .show(100, truncate=False)


def display_selected_columns(df: DataFrame):
    """
    Displays only selected columns.
    """
    print("\n========== SELECTED COLUMNS ==========\n")

    df.select(
        "city",
        "state"
    ).show(10, truncate=False)


def display_filtered_state(df: DataFrame, state: str):
    """
    Displays records belonging to a specific state.
    """
    print(f"\n========== FILTERED DATA ({state}) ==========\n")

    df.filter(
        F.col("state") == state
    ).show(10, truncate=False)


def display_sorted_by_city(df: DataFrame):
    """
    Displays data sorted by city.
    """
    print("\n========== SORTED BY CITY ==========\n")

    df.orderBy("city").show(10, truncate=False)


def select_final_columns(df: DataFrame) -> DataFrame:
    """
    Selects and orders the columns for the final dataset.
    """
    print("\n========== SELECTING FINAL COLUMNS ==========\n")

    final_df = df.select(
        "city",
        "state",
        "shape_reported",
        "colors_reported",
        "color_count",
        "time",
        "date",
        "year",
        "month",
        "month_name",
        "day_of_week",
        "hour"
    )

    return final_df

def final_data_verification(df: DataFrame):
    """
    Performs final checks before loading the dataframe.
    """

    print("\n========== FINAL DATA VERIFICATION ==========\n")

    print(f"Rows: {df.count()}")
    print(f"Columns: {len(df.columns)}")

    print("\nSchema:")
    df.printSchema()

    print("\nNULL VALUES:")

    null_counts = df.select([
        F.count(
            F.when(F.col(column).isNull(), column)
        ).alias(column)
        for column in df.columns
    ])

    null_counts.show(truncate=False)