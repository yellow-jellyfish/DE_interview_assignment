import argparse
import logging

from typing import Optional
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def parse_args():
    parser = argparse.ArgumentParser(description='Process csv files.')
    parser.add_argument('--dataset1', required=True, help='Path to the first dataset.')
    parser.add_argument('--dataset2', required=True, help='Path to the second dataset.')
    parser.add_argument('--countries', required=True, nargs='+', help='Countries to filter.')
    args = parser.parse_args()
    return args.dataset1, args.dataset2, args.countries


# one branch for testing one for logging 
# do good commit messages "conventional commit" 
# do good names 
# sdf
# autodocstring


def drop_columns(sdf, cols):
    sdf_dropped = sdf.drop(*cols)
    #df_dropped.printSchema()
    return sdf_dropped
#compare schema 


def join_dataframes(sdf1,sdf2):
    sdf_merged = sdf1.join(sdf2,
               sdf1.id == sdf2.id,
               "inner")
    sdf_merged = sdf_merged.drop(sdf2.id)
    #sdf_merged.printSchema()
    return sdf_merged

def filter_countries(sdf, countries):
    sdf_filtered = sdf.filter(sdf.country.isin(countries))
    return sdf_filtered

def main():
    spark=SparkSession.builder.appName('data_preprocessing').getOrCreate()
    logging.info('Spark session created')

    # Get arguments from command line
    dataset1_path, dataset2_path, countries = parse_args()
    logging.info(f"Arguments: dataset1={dataset1_path}, dataset2={dataset2_path}, countries={countries}")

    # Read CSV files into Spark DataFrames
    sdf1 = spark.read.csv(dataset1_path, header=True, inferSchema=True)
    sdf2 = spark.read.csv(dataset2_path, header=True, inferSchema=True)
    logging.info('CSV files read into Spark DataFrames')

    sdf_merged = join_dataframes(sdf1,sdf2)
    sdf_merged = drop_columns(sdf_merged, ["first_name", "last_name", "cc_n"])


    #df_merged_pyspark.select('country').distinct().show()


    sdf_filtered = filter_countries(sdf_merged, countries)
    logging.info('Data transformations completed')
    #df_filtered.select('country').distinct().show()

    sdf_final = sdf_filtered.selectExpr("id as client_identifier","btc_a as bitcoin_address","cc_t as credit_card_type","country", "email")
    sdf_final.printSchema()

    sdf_final.write.mode('overwrite').option("header",True) \
    .csv("client_data")
    logging.info('Data written to client_data.csv')


if __name__ == '__main__':
    main()


