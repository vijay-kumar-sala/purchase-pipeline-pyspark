from pyspark.sql import SparkSession
# from pyspark.sql.functions import expr
import logging
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'../../configuration')))

from custom_logging import set_logging

set_logging()
logger = logging.getLogger('transformlog')

def format_query_str(cols,fun):
    
    query_form = ""

    for i in range(0,len(cols)):
        if(fun == "sum"):
            query_form += " sum({}) as {},".format(cols[i],cols[i])
        if(fun == "avg"):
            query_form += " avg({}) as {},".format(cols[i],cols[i])
        if(fun == "min"):
            query_form += " min({}) as {},".format(cols[i],cols[i])
        if(fun == "max"):
            query_form += " max({}) as {},".format(cols[i],cols[i])
        if(fun == "count"):
            query_form += " count({}) as {},".format(cols[i],cols[i])
    return query_form

def grpByKeyAgg(spark, df, key, sum=None, avg=None, min=None, max=None, count=None):

    try:
        if(key!=None):

            queryString = "SELECT {},".format(key)

            if(sum!=None):
                queryString += format_query_str(sum.split(','),"sum")
            if(avg!=None):
                queryString += format_query_str(avg.split(','),"avg")
            if(min!=None):
                queryString += format_query_str(min.split(','),"min")
            if(max!=None):
                queryString += format_query_str(max.split(','),"max")
            if(count!=None):
                queryString += format_query_str(count.split(','),"count")
            
            queryString = queryString[:-1] + ' '
            queryString += "FROM tempView GROUP BY {}".format(key)

            logger.warn("Printing query string: {}".format(queryString))
            df.createOrReplaceTempView("tempView")
            grouped_df = spark.sql(queryString)

            return grouped_df

    except Exception as e:

        logger.error("exception {} while grouping and aggregation on df".format(e))

def join_df(spark, left_df,right_df, Joining_condition, joining_type):

    try:
        if(Joining_condition!=None and joining_type!=None):
            
            left_df.show()
            left_df.createTempView("leftView")
            right_df.show()
            right_df.createTempView("rightView")

            queryString = "SELECT leftView.*, rightView.* FROM leftView {} JOIN rightView ON {}".format(joining_type, Joining_condition)
            
            return spark.sql(queryString)

    except Exception as e:
        logger.error("exception {} while joining two dataframes, please cehck joining condition and type".format(e))


        