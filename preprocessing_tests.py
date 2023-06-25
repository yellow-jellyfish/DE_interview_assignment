# create SparkSession
import unittest
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.types import StructType, StructField, StringType
from data_preprocessing import drop_columns, join_dataframes, filter_countries



class JoinDataframesTest(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .master("local[*]") \
            .appName("chispa") \
            .getOrCreate()

    def test_join_empty_dataframes(self):
        # Create empty dataframes
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
        #get dataframe resulting from join function
        joined_sdf = join_dataframes(empty_sdf1,empty_sdf2)
        #check the result
        assert_df_equality(joined_sdf,expected_sdf)

#    def test_join_duplicate_ids(self):

#    def test_join_no_common_ids(self):
 
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

if __name__ == '__main__':
    unittest.main()

