from pyspark.sql import SparkSession
from validate import val_spark_obj, val_feed, val_schema
from transform import merge_columns, add_columns
from load import write_to_sink
from data_parser import DataParser
import logging
import argparse
import sys
import os
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'../../')))

from configuration.custom_logging import set_logging

set_logging()
logger = logging.getLogger('applog')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--app_name")
    parser.add_argument("--input_path")
    parser.add_argument("--sink")
    parser.add_argument("--bad_rows_collection")
    parser.add_argument("--staging_area")
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

            filename = file_path.split('/')[-1].split('.')[0]
            if(filename.endswith("Data")):
                filename=filename[0:filename.index("Data")]
            df_name = filename+"_df"
            DataParser_obj = DataParser(filename, file_path, df_name)
            # read data
            DataParser_obj.file_reader(spark)
            DataParser_obj.get_df().show()


            if(DataParser_obj.schema_validation()):

                # data validation, cleaning
                DataParser_obj.data_validation()
                if(DataParser_obj.bad_rows!=None):
                    logger.info("bad rows count: {}".format(DataParser_obj.bad_rows.count()))
                    logger.info("{}".format(DataParser_obj.bad_rows.show()))
                    col_delimiter=','
                    merging_cols=["*"]
                    bad_merged_rows = merge_columns(spark,DataParser_obj.bad_rows,col_delimiter,merging_cols)
                    bad_merged_rows.show()
                    new_col_def = {
                        0:{
                            "column_name":"filename",
                            "expression":file_path
                        },
                        1:{
                            "column_name":"dateTime",
                            "expression":"now"
                        }
                    }

                    bad_merged_rows = add_columns(spark, bad_merged_rows, new_col_def)
                    if(bad_merged_rows.count()>0):
                        DataParser_obj.write_data(write_flag="bad_rows_write", sink=args.sink,sink_path=args.bad_rows_collection,connection_uri=args.connection_uri,write_disposition="WRITE_APPEND",create_disposition="CREATE_IF_NEEDED", df=bad_merged_rows)

                # staging
                if(DataParser_obj.get_df().count()>0):
                    logger.info("dataframe count post cleaning: {}".format(DataParser_obj.get_df().count()))
                    DataParser_obj.write_data(write_flag="df_rows_write", sink=args.sink,sink_path=args.staging_area,connection_uri=args.connection_uri,write_disposition=args.write_disposition, create_disposition=args.create_disposition)

            '''
            if(users_df!=None and purchase_df!=None):

                users_quantity_df = grpByKeyAgg(spark=spark, df=purchase_df,key="user_id",sum="Quantity")
                users_quantity_df.show()
                joined_df = join_df(spark=spark, left_df=users_df, right_df=users_quantity_df, Joining_condition="leftView.user_id = rightView.user_id", joining_type="inner")

                res_df = grpByKeyAgg(spark=spark,df=joined_df,key="user_address",sum="Quantity")

                # load data
                write_to_sink(df=res_df, sink=args.sink, sink_path=args.output_path, connection_uri=args.connection_uri, write_disposition=args.write_disposition, create_disposition=args.create_disposition)
            '''
        spark.stop()
        logger.info("spark application stopped")



if __name__ == "__main__":

    main()
