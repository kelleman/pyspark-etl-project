from src.spark_session import create_spark_session
from src.extract import read_csv

from src.transform import (
    # Data Inspection
    display_schema,
    display_basic_information,
    preview_data,
    descriptive_statistics,

    # Data Cleaning
    display_null_values,
    display_empty_values,
    display_duplicates,
    display_duplicate_rows,
    fill_null_values,
    remove_duplicate_rows,

    # Data Transformation
    standardize_column_names,
    convert_time_to_timestamp,
    extract_time_information,
    display_selected_columns,
    display_filtered_state,
    display_sorted_by_city,
)


def main():

    # ==========================================================
    # CREATE SPARK SESSION
    # ==========================================================

    spark = create_spark_session()

    try:

        # ======================================================
        # EXTRACT
        # ======================================================

        df = read_csv(
            spark,
            "data/raw/ufo.csv"
        )


        # ======================================================
        # DATA INSPECTION
        # ======================================================

        display_schema(df)

        display_basic_information(df)

        preview_data(df)

        descriptive_statistics(df)


        # ======================================================
        # DATA CLEANING - INSPECTION
        # ======================================================

        # Check NULL values before cleaning
        display_null_values(df)

        # Check empty string values
        display_empty_values(df)

        # Check duplicate rows
        display_duplicates(df)

        # Display duplicate rows
        display_duplicate_rows(df)


        # ======================================================
        # DATA CLEANING
        # ======================================================

        # Replace selected NULL values with "Unknown"
        df = fill_null_values(df)
        
        # Remove duplicate rows
        remove_duplicate_rows(df)


        # ======================================================
        # DATA CLEANING - VERIFICATION
        # ======================================================

        # Check NULL values again after cleaning
        display_null_values(df)


        # ======================================================
        # DATA TRANSFORMATION
        # ======================================================

        # Standardize column names
        df = standardize_column_names(df)

        # Convert time column to timestamp
        df = convert_time_to_timestamp(df)

        # Extract year, month, and hour
        df = extract_time_information(df)

        # Display selected columns
        display_selected_columns(df)

        # Filter records by state
        display_filtered_state(df, "CA")

        # Sort records by city
        display_sorted_by_city(df)


    finally:

        # ======================================================
        # STOP SPARK SESSION
        # ======================================================

        spark.stop()


if __name__ == "__main__":
    main()