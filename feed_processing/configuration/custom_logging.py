import logging.config
# import os
# import sys


def set_logging():

    logging.config.fileConfig('feed_processing/configuration/logger.config', disable_existing_loggers=False)