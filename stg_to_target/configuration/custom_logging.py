import logging.config
# import os
# import sys


def set_logging():

    logging.config.fileConfig('/home/kumar/datapipeline-pyspark/stg_to_target/configuration/logger.config', disable_existing_loggers=False)