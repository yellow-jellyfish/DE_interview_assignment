# create SparkSession
import unittest
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.types import StructType, StructField, StringType
from data_preprocessing import drop_columns, join_dataframes, filter_countries


class ProcessingDataTests(unittest.TestCase): 
    @classmethod
    def setUpClass(cls):
        """
        Sets up the testing environment.
        """        
        cls.spark = SparkSession.builder \
            .master("local[*]") \
            .appName("chispa") \
            .getOrCreate()
        

    def test_join_empty_dataframes(self):
        """
        Tests 'join_dataframes' function when input dataframes are empty.
        The test asserts that the function returns an empty dataframe with 
        the correct schema when joining two empty dataframes.

        """
        # Define the schema
        schema1 = StructType([
            StructField("id", StringType(), True),
            StructField("email", StringType(), True),
            StructField("country", StringType(), True)
        ])
        schema2 = StructType([
            StructField("id", StringType(), True),
            StructField("btc_a", StringType(), True),
        ])

        # Create empty DataFrames using the schema
        empty_sdf1 = self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema1)
        empty_sdf2 = self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema2)
        
        #create expected dataframe
        schema3 = StructType([
            StructField("id", StringType(), True),
            StructField("email", StringType(), True),
            StructField("country", StringType(), True),
            StructField("btc_a", StringType(), True)
        ])    
        expected_sdf= self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema3)
        
        #Get dataframe resulting from join function
        joined_sdf = join_dataframes(empty_sdf1,empty_sdf2)
        
        #Check the result
        assert_df_equality(joined_sdf,expected_sdf)

        
    def test_join_no_common_ids(self):
        """
        Tests 'join_dataframes' function when input dataframes have no common id.
        The test asserts that the function returns an empty dataframe with 
        the correct schema when joining dataframes with no common id.
        """
        # Define the schema
        schema1 = StructType([
            StructField("id", StringType(), True),
            StructField("email", StringType(), True),
            StructField("country", StringType(), True)
        ])
        
        schema2 = StructType([
            StructField("id", StringType(), True),
            StructField("btc_a", StringType(), True),
        ])
            
        # Create the input DataFrames with no common IDs
        sdf1 = self.spark.createDataFrame(
            [("1", "email1@example.com", "Country1")], schema=schema1
        )
        sdf2 = self.spark.createDataFrame(
            [("2", "btc_a2")], schema=schema2
        )
            
        #create expected empty dataframe 
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("email", StringType(), True),
            StructField("country", StringType(), True),
            StructField("btc_a", StringType(), True)
        ])    
        expected_sdf= self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema)

        # Get the resulting DataFrame from the join function
        joined_sdf = join_dataframes(sdf1, sdf2)
            
        # Check the result
        assert_df_equality(joined_sdf, expected_sdf)
    
    def test_drop_non_existent_columns(self):
        """
        Tests 'drop_columns' function when input dataframes have no common id.
        The test asserts that the function returns an empty dataframe with 
        the correct schema when joining dataframes with no common id.
        """
        source_data = [
            ("1", "email", "US"),
            ("2", "gmail", "Russia"),
        ]
        sdf = self.spark.createDataFrame(
            source_data, 
            ["id", "email", "country"]
        )
        dropped_sdf = drop_columns(sdf, ["non_existent_column"])
        
        # The DataFrame should be unchanged since the column didn't exist
        assert_df_equality(sdf, dropped_sdf)


    def test_filter_non_existent_countries(self):
        """
        Tests 'filter_countries' function when input countries don't occur withing the dataframe.
        The test asserts that the function returns an empty dataframe with 
        the correct schema when countries don't occur withing the dataframe.
   
        """        
        source_data = [
            ("1", "email", "US"),
            ("2", "gmail", "Russia"),
        ]
        sdf = self.spark.createDataFrame(
            source_data, 
            ["id", "email", "country"]
        )
        filtered_sdf = filter_countries(sdf, ["NonExistentCountry"])
        
        # The DataFrame should be empty since no country matched
        schema = StructType([
                StructField("id", StringType(), True),
                StructField("email", StringType(), True),
                StructField("country", StringType(), True),
            ])    
        expected_sdf= self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema)
        
        assert_df_equality(expected_sdf, filtered_sdf)
  
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

if __name__ == '__main__':
    unittest.main()

