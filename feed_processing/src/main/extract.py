from pyspark.sql import SparkSession
import sys
import os
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'../../configuration')))

from custom_logging import set_logging

set_logging()
logger = logging.getLogger('readfilelog')

def extract_data():

    return null