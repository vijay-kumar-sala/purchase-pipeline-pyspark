from pyspark.sql import SparkSession
from read_file import file_reader
from validate import val_spark_obj, val_feed, val_schema, val_data_cleaning
from transform import grpByKeyAgg, join_df
from load import write_to_sink
import logging
import argparse
import sys
import os
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'../../')))

from configuration.custom_logging import set_logging

with open("feed_processing/data_model/usersModel.json","r") as jsonFile:
    usersModel = json.load(jsonFile)

with open("feed_processing/data_model/purchaseModel.json","r") as jsonFile:
    purchaseModel = json.load(jsonFile)

set_logging()
logger = logging.getLogger('applog')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--app_name")
    parser.add_argument("--input_path")
    parser.add_argument("--sink")
    parser.add_argument("--output_path")
    parser.add_argument("--connection_uri")
    parser.add_argument("--write_disposition")
    parser.add_argument("--create_disposition")
    args = parser.parse_args()

    logger.info("started spark application")

    spark = SparkSession\
                .builder\
                .appName(args.app_name)\
                .getOrCreate()

    # validating spark session objj
    if(val_spark_obj(spark)):

        inputFilePaths = args.input_path.split(',')
        
        for file_path in inputFilePaths:

            if(not val_feed(spark, file_path)):
                return False
        # read data
        users_df = file_reader(spark=spark, path=inputFilePaths[0], file_extension=inputFilePaths[0].split('.')[-1])

        purchase_df = file_reader(spark=spark, path=inputFilePaths[1],file_extension=inputFilePaths[1].split('.')[-1])

        if(val_schema(actualModel=purchaseModel,df=purchase_df) and val_schema(actualModel=usersModel,df=users_df)):

            # transform
            users_df.printSchema()
            purchase_df.printSchema()

            if(val_data_cleaning(actualModel=purchaseModel,df=purchase_df) and val_data_cleaning(actualModel=usersModel,df=users_df)):
                users_quantity_df = grpByKeyAgg(spark=spark, df=purchase_df,key="user_id",sum="Quantity")
                users_quantity_df.show()
                joined_df = join_df(spark=spark, left_df=users_df, right_df=users_quantity_df, Joining_condition="leftView.user_id = rightView.user_id", joining_type="inner")

                res_df = grpByKeyAgg(spark=spark,df=joined_df,key="user_address",sum="Quantity")

                # load data
                write_to_sink(df=res_df, sink=args.sink, sink_path=args.output_path, connection_uri=args.connection_uri, write_disposition=args.write_disposition, create_disposition=args.create_disposition)

        spark.stop()
        logger.info("spark application stopped")



if __name__ == "__main__":

    main()
