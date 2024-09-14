from pyspark.sql import SparkSession

def val_spark_obj(spark):
    
    try:
        print("------------------------------------------------------------------------")
        print("validated spark object: {}".format(spark.sql("SELECT current_timestamp()")))

    except Exception as e:
        print("exception {} while validating spark session obj".format(e))