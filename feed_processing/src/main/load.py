from pyspark.sql import SparkSession
import logging
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'../../configuration')))

from custom_logging import set_logging

set_logging()
logger = logging.getLogger('loadlog')

def write_to_sink(df, sink, sink_path, file_extension=None, connection_uri=None, write_disposition="append", create_disposition=None):

    try:
        if(sink == "file"):
            to_file(df, sink_path, file_extension)
        
        if(sink == "bq"):
            to_bq(df, sink_path, connection_uri, write_disposition, create_disposition)
        
        if(sink == "mongo"):
            to_mongo(df, sink_path, connection_uri, write_disposition,create_disposition)

    except Exception as e:
        logger.error("exception {} while writing to sink {}".format(e,sink))


def to_file(df, path, ext):
    try:
        if(ext == "json"):
            logger.warn("writing df to json file")
            df.write\
                .json(path)
        if(ext == "csv"):
            logger.warn("writing df to csv file")
            df.write\
                .csv(path)
        if(ext == "txt"):
            logger.warn("writing df to text file")
            df.write\
                .text(path)
    except Exception as e:
        logger.error("exception {} while write to file".format(e))

def to_bq(df, path, connection_uri, write_disposition, create_disposition):
    try:
        logger.warn('writing to bigquery')
        project_id = connection_uri
        df.write\
            .format("bigquery")\
            .option("table",path)\
            .option("create_disposition",create_disposition)\
            .mode(write_disposition)\
            .save()
    except Exception as e:
        logger.error("exception {} while write to bigquery".format(e))

def to_mongo(df, path, connection_uri, write_disposition, create_disposition):
    try:
        logger.warn('wrtiting to mongodb')
        df.write\
            .format("mongodb")\
            .option("spark.mongodb.write.connection.uri",connection_uri+path)\
            .mode(write_disposition)\
            .save()
    
    except Exception as e:
        logger.error('exception {} while write to mongodb'.format(e))