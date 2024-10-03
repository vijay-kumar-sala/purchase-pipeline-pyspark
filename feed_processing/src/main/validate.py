from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
import logging
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'../../configuration')))

from custom_logging import set_logging

set_logging()
logger = logging.getLogger('validatelog')


def val_spark_obj(spark):
    
    try:

        logger.info("validated spark object: {}".format(spark.sql("SELECT current_date").collect()))
        return True

    except Exception as e:
        logger.error("exception {} while validating spark session obj".format(e))
        return False


def val_feed(input_file):

    try:
        logger.info("validating input file {} at {}".format(input_file,current_date()))
        size = os.path.getsize(input_file)
        logger.info("file size is: {}".format(size))

        if(size==0):
            raise "input file size is 0B, couldn't process it."
            
        return True

    except Exception as e:
        logger.warn("exception {} while file validation, check input_file and its path".format(e))
        return False

# data validation

def val_schema(actualModel,df):

    try:
            
        logger.info("Inside schema validation for {}".format(df))
        # logger.info("printing schema {}".format(df.printSchema()))
        
        df_columns = df.columns
        actualModelDict = actualModel["schema"]
        model_keys = list(actualModelDict.keys())
        if(len(df_columns)==len(model_keys)):
            model_keys.sort()
            df_columns.sort()
            df_columns_types = df.dtypes
            for i in range(0,len(df_columns)):
                if(df_columns[i]!=model_keys[i]):

                    logger.warn("schema not match with actual df columns for {} model {} df particularly {} != {}".format(model_keys,df_columns,model_keys[i],df_columns[i]))
                    return False
                if(df_columns_types[i][1]!=actualModelDict[df_columns_types[i][0]]['type']):
                    logger.warn("type not match with actual df columns for {} model {} df {}--{} columns, {}--{}types".format(actualModel,df,model_keys[i],df_columns[i],df_columns_types[i][1],actualModelDict[df_columns_types[i][0]]['type']))
                    return False
        else:
            logger.warn("noof df columns not match with actual model columns for {} model {} df".format(actualModel,df))
            return False

    except Exception as e:
        logger.error("exception {} while schema validation".format(e))
        return False

    logger.info("successfully schema validation done")
    return True