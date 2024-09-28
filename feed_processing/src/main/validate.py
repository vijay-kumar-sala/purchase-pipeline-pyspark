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

        logger.info("validated spark object: {}".format(spark.sql("SELECT current_date")))
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