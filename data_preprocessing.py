import argparse
import logging
from typing import Optional
from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def parse_args():
    """
    Parses command-line arguments required for processing CSV files.

    Returns:
        tuple: A tuple containing the paths to the first and second datasets as strings,
               and a list of countries to filter as the third element.
    """
    parser = argparse.ArgumentParser(description="Process csv files.")
    parser.add_argument("--dataset1", required=True, help="Path to the first dataset.")
    parser.add_argument("--dataset2", required=True, help="Path to the second dataset.")
    parser.add_argument("--countries", required=True, nargs="+", help="Countries to filter.")
    args = parser.parse_args()
    return args.dataset1, args.dataset2, args.countries


def drop_columns(sdf, cols):
    """Drops specified columns from the input Spark DataFrame.

    Args:
        sdf (pyspark.sql.DataFrame): The original DataFrame to modify
        cols (list): List of column names (strings) to drop from the DataFrame

    Returns:
        pyspark.sql.DataFrame: New DataFrame without the specified columns. 
    """
    existing_columns = set(sdf.columns)
    cols_to_drop = [col for col in cols if col in existing_columns]

    sdf_dropped = sdf.drop(*cols_to_drop)
    return sdf_dropped


def join_dataframes(sdf1, sdf2):
    """Inner joins two Spark DataFrames based on id column.

    Args:
        sdf1 (pyspark.sql.DataFrame): First original DataFrame to modify. Must contain id column.
        sdf2 (pyspark.sql.DataFrame): Second original DataFrame to modify. Must contain id column.

    Returns:
        pyspark.sql.DataFrame: Joind DataFrame containing ids present in both original DataFrames.
    """
    sdf_merged = sdf1.join(sdf2, sdf1.id == sdf2.id, "inner")
    sdf_merged = sdf_merged.drop(sdf2.id)
    return sdf_merged


def filter_dataframe(sdf,filter_values, filter_column="country"):
    """Filters the spark DataFrame to only contain rows with specified values in a chosen column.
    Default column is country. 

    Args:
        sdf (pyspark.sql.DataFrame): The original spark DataFrame. 
        filter_values (list): List of value names (strings) to filter on. If no matching values are found, the function returns an empty DataFrame.
        filter_column (string): Name of column in the orignal spark DataFrame to filter. 
    Returns:
        pyspark.sql.DataFrame: A new DataFrame containing only rows where the filter_column value
                               is present in the provided list of values.
    """
    if not filter_values:
        return sdf
    sdf_filtered = sdf.filter(sdf[filter_column].isin(filter_values))
    return sdf_filtered


def main():
    """Main function to execute the data preprocessing program."""

    spark = SparkSession.builder.appName("data_preprocessing").getOrCreate()
    logging.info("Spark session created")

    # Get arguments from command line
    dataset1_path, dataset2_path, countries = parse_args()
    logging.info(
        f"Arguments: dataset1={dataset1_path}, dataset2={dataset2_path}, countries={countries}"
    )

    # Read CSV files into Spark DataFrames
    sdf1 = spark.read.csv(dataset1_path, header=True, inferSchema=True)
    sdf2 = spark.read.csv(dataset2_path, header=True, inferSchema=True)
    logging.info("CSV files read into Spark DataFrames")

    sdf_merged = join_dataframes(sdf1, sdf2)
    sdf_merged = drop_columns(sdf_merged, ["first_name", "last_name", "cc_n"])

    sdf_filtered = filter_dataframe(sdf_merged, countries)
    logging.info("Data transformations completed")

    sdf_final = sdf_filtered.selectExpr(
        "id as client_identifier",
        "btc_a as bitcoin_address",
        "cc_t as credit_card_type",
        "country",
        "email",
    )
    sdf_final.printSchema()

    sdf_final.write.mode("overwrite").option("header", True).csv("client_data")
    logging.info("Data written to client_data.csv")


if __name__ == "__main__":
    main()
