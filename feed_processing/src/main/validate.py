from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, col
from py4j.java_gateway import java_import
import logging
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'../../configuration')))

from custom_logging import set_logging

set_logging()
logger = logging.getLogger('validatelog')


def val_spark_obj(spark):
    
    try:

        logger.info("validated spark object: {}".format(spark.sql("SELECT current_date as date").collect()))
        return True

    except Exception as e:
        logger.error("exception {} while validating spark session obj".format(e))
        return False


def val_feed(spark, input_file_path):

    try:
        logger.info("validating input file {} at {}".format(input_file_path,current_date()))

        s_context = spark.sparkContext

        java_import(s_context._gateway.jvm,"org.apache.hadoop.fs.FileSystem")
        java_import(s_context._gateway.jvm,"org.apache.hadoop.fs.Path")

        hadoop_conf = s_context._jsc.hadoopConfiguration()
        print("done1")
        hdfs = s_context._gateway.jvm.FileSystem.get(hadoop_conf)
        print("done2",type(hdfs))

        path = s_context._gateway.jvm.Path(input_file_path)
        print("done3")
        file_status = hdfs.getFileStatus(path)
        print("done4")

        size = file_status.getLen()
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
            df.show()
            logger.warn("noof df columns not match with actual model columns for {} model {} df".format(actualModel,df))
            return False

    except Exception as e:
        logger.error("exception {} while schema validation".format(e))
        return False

    logger.info("successfully schema validation done")
    return True


def val_data_cleaning(actualModel,df):

    try:

        logger.info("validating data for df {}".format(df))

        df.show()
        df_columns = df.columns
        actualModel_dict = actualModel["schema"]

        for column in df_columns:

            if(actualModel_dict[column]["required"]=="true"):
                col_nulls_from_df=df.where(col(column).isNull())
                col_nulls_from_df.show()

                if(col_nulls_from_df.count()!=0):
                    logger.warn("having null value for required field {}".format(column))
                    logger.warn("rows with null values for above field: {} \n removing rows with null".format(col_nulls_from_df.collect()))
                    df=df.where(col(column).isNotNull())

                else:
                    logger.info(f"No null values for required field {column}")
        return df

    except Exception as e:
        logger.error("Exception {} while data validation at checking null values for required fields.".format(e))
        return False