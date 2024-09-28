from pyspark.sql import SparkSession
import sys
import os
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'../../configuration')))

from custom_logging import set_logging

set_logging()
logger = logging.getLogger('readfilelog')

# reading data from files
# parse spark obj, path to read file, file extension
def file_reader(spark, path, file_extension):
    if(file_extension=="json"):
        return read_json(spark, path)
    elif(file_extension=="csv"):
        inferSchema = True
        header = True
        return read_csv(spark, path,header,inferSchema)
    elif(file_extension=="txt"):
        return read_text(spark, path)

# read json file
def read_json(spark, path):
    try:
        logger.info("reading json file")
        return spark\
                    .read\
                    .json(path)
    except Exception as e:
        logger.error("exception {} while reading json file".format(e))

# read csv file
def read_csv(spark, path,header,inferSchema):
    try:
        logger.info("reading csv file")
        return spark\
                    .read\
                    .option("header",header)\
                    .option("inferschema",inferSchema)\
                    .csv(path)
    except Exception as e:
        logger.error("exception {} while reading csv file".format(e))

# read text file
def read_text(spark, path):
    try:
        logger.info("reading text file")
        return spark\
                    .read\
                    .text(path)
    except Exception as e:
        logger.error("exception {} while reading text file".format(e))


# def read_parquet()ss