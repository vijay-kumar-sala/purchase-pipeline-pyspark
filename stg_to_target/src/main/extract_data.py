from pyspark.sql import SparkSession
import sys
import os
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'../../configuration')))

from custom_logging import set_logging

set_logging()
logger = logging.getLogger('extractdatalog')

def extract_from_mongodb(spark, database, collection, volume, connection_uri, increment_condition=None):

    try:
        logger.info("reading from mongodb")
        df = None
        if(volume=="full"):
            df_reader = spark.read
            df = df_reader.format("mongodb")\
                            .option("spark.mongodb.read.connection.uri",connection_uri)\
                            .option("database",database)\
                            .option("collection",collection)\
                            .load()
        
        if(volume=="increment"):
            df = spark.read\
                    .format("mongodb")\
                    .option("spark.mongodb.read.connection.uri",connection_uri)\
                    .option("database",database)\
                    .option("collection",collection)\
                    .option("pipeline",increment_condition)\
                    .load()
        
        if(df==None):
            logger.warn("No data to process!")
        return df

    except Exception as e:
        logger.error("Exception {} while extraction from {}".format(e,collection))
        return None