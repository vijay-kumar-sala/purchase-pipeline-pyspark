import logging
import sys
import os
import argparse
import json
from extract_data import *
from pyspark.sql import SparkSession

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'../../..')))

from stg_to_target.configuration.custom_logging import set_logging
from feed_processing.src.main.transform import groupByKey, Combine, coGroupByKey, join_df, flatten
from feed_processing.src.main.load import write_to_sink
from feed_processing.src.main.validate import val_spark_obj
set_logging()
logger = logging.getLogger("applog")

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--app_name")
    parser.add_argument("--target_load_config")
    parser.add_argument("--packages")
    parser.add_argument("--connection_uri")
    args = parser.parse_args()
    

    spark = SparkSession\
                .builder\
                .appName("target load application")\
                .getOrCreate()
    if(not val_spark_obj(spark)):
        return 
    logger.info("spark application started")


    with open("/home/kumar/datapipeline-pyspark/stg_to_target/configuration/target_config/"+args.target_load_config) as json_file:
        target_config = json.load(json_file)

    # extract
    
    dfs_dict = {}
    if(target_config["source_type"]=="mongodb"):
        input_collections_cnt = len(target_config["source_collections"])
        for i in range(0, input_collections_cnt):
            if(i<len(target_config["data_extraction"]) and target_config["source_collections"][i]==target_config["data_extraction"][i]["collection"]):
                df = extract_from_mongodb(spark, target_config["data_extraction"][i]["database"], target_config["source_collections"][i], target_config["data_extraction"][i]["volume"], args.connection_uri)
                if(df==None):
                    return
                dfs_dict.update({target_config["source_collections"][i]+"_df":df})

    # transform
    for transformation in target_config["transformations"]:

        if("groupByKey" in transformation):
            dfs_dict.update({transformation["groupByKey"]["resultant_name"]: groupByKey(spark, dfs_dict[transformation["groupByKey"]["on_df"]], transformation["groupByKey"]["keys"])})
        if("Combine" in transformation):
            dfs_dict.update({transformation["Combine"]["resultant_name"]: Combine(spark, dfs_dict[transformation["Combine"]["on_df"]], transformation["Combine"]["keys"],transformation["Combine"]["agg"])})
        if("coGroupByKey" in transformation):
            dfs_list = []
            for df in transformation["coGroupByKey"]["on_dfs"]:
                dfs_list.append(dfs_dict.get(df))
            dfs_dict.update({transformation["coGroupByKey"]["resultant_name"]: coGroupByKey(spark, dfs_list,transformation["coGroupByKey"]["keys"])})
        if("join" in transformation):
            left_df = dfs_dict.get(transformation["join"]["left_df"])
            right_df = dfs_dict.get(transformation["join"]["right_df"])
            dfs_dict.update({transformation["join"]["resultant_name"]: join_df(spark, transformation["join"]["left_df"], left_df,transformation["join"]["right_df"], right_df, transformation["join"]["joining_on"], transformation["join"]["joining_type"])})
        if("flatten" in transformation):
            up_df = dfs_dict.get(transformation["flatten"]["A_df"])
            down_df = dfs_dict.get(transformation["flatten"]["B_df"])
            dfs_dict.update({transformation["flatten"]["resultant_name"]: flatten(spark, up_df, down_df)})

    # loading
    if(target_config["load"]["sink"]=="mongodb"):
        resDF = target_config["load"]["loading_df"]
        print(dfs_dict[resDF])
        if((resDF in dfs_dict and dfs_dict[resDF]!=None) and dfs_dict[resDF].count()>0):
            dfs_dict[resDF].show()
            write_to_sink(df=dfs_dict[resDF],sink="mongo",sink_path=target_config["load"]["output_path"],connection_uri= args.connection_uri,write_disposition=target_config["load"]["loading_type"],create_disposition=None)
    
    '''
    if src type bq

    if src type csql'''

    spark.stop()

if __name__=="__main__":

    main()