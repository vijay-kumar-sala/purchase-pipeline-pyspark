from pyspark.sql import SparkSession
from read_file import file_reader
from validate import val_spark_obj
from transform import grpByKeyAgg, join_df
from load import write_to_sink
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--app_name")
parser.add_argument("--input_path")
parser.add_argument("--sink")
parser.add_argument("--output_path")
parser.add_argument("--connection_uri")
parser.add_argument("--write_disposition")
parser.add_argument("--create_disposition")
args = parser.parse_args()


spark = SparkSession\
            .builder\
            .appName(args.app_name)\
            .getOrCreate()

# validating spark session objj
val_spark_obj(spark)

inputFilePaths = args.input_path.split(',')

# read data
users_df = file_reader(spark, inputFilePaths[0], inputFilePaths[0].split('.')[-1])

purchase_df = file_reader(spark, inputFilePaths[1],inputFilePaths[1].split('.')[-1])


# transform
users_df.printSchema()
purchase_df.printSchema()


users_quantity_df = grpByKeyAgg(spark, purchase_df, "user_id", "Quantity", None, None, None, None )
users_quantity_df.show()

joined_df = join_df(spark, users_df, users_quantity_df, "leftView.user_id = rightView.user_id", "inner")

res_df = grpByKeyAgg(spark, joined_df, "user_address", "Quantity", None, None, None, None)

# load data
write_to_sink(res_df, args.sink, args.output_path, args.output_path.split('.')[-1], args.connection_uri, args.write_disposition, args.create_disposition)


spark.stop()
