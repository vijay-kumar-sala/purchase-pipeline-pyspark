from pyspark.sql import SparkSession

def write_to_sink(df, sink, sink_path, file_extension, connection_uri, write_disposition, create_disposition):

    try:
        if(sink == "file"):
            to_file(df, sink_path, file_extension)
        
        if(sink == "bq"):
            to_bq(df, sink_path, connection_uri, write_disposition, create_disposition)
        
        if(sink == "mongo"):
            to_mongo(df, sink_path, connection_uri, write_disposition,create_disposition)

    except Exception as e:
        print("exception {} while writing to sink {}".format(e,sink))


def to_file(df, path, ext):

    if(ext == "json"):
        df.write\
            .json(path)
    if(ext == "csv"):
        df.write\
            .csv(path)
    if(ext == "txt"):
        df.write\
            .text(path)

def to_bq(df, path, connection_uri, write_disposition, create_disposition):

    project_id = connection_uri
    df.write\
        .format("bigquery")\
        .option("table",path)\
        .option("create_disposition",create_disposition)\
        .mode(write_disposition)\
        .save()


def to_mongo(df, path, connection_uri, write_disposition, create_disposition):

    df.write\
        .format("mongodb")\
        .option("spark.mongodb.write.connection.uri",connection_uri+path)\
        .mode(write_disposition)\
        .save()