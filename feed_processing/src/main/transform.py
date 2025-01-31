from pyspark.sql import SparkSession
from pyspark.sql.DataFrame import select, withColumns
import logging
import sys
import os
import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'../../configuration')))

from custom_logging import set_logging

set_logging()
logger = logging.getLogger('transformlog')

def format_query_str_agg_funs(cols,fun):
    
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
                queryString += format_query_str_agg_funs(sum.split(','),"sum")
            if(avg!=None):
                queryString += format_query_str_agg_funs(avg.split(','),"avg")
            if(min!=None):
                queryString += format_query_str_agg_funs(min.split(','),"min")
            if(max!=None):
                queryString += format_query_str_agg_funs(max.split(','),"max")
            if(count!=None):
                queryString += format_query_str_agg_funs(count.split(','),"count")
            
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


def merge_columns(spark,df,col_delimiter,merging_cols):

    try:
        
        df_columns = df.columns
        df.createOrReplaceTempView("mergeRowsView")
        if(type(merging_cols)==str):
            merging_cols = merging_cols.split(',')
        if(merging_cols==["*"]):
            queryString = "Select concat("
            for col in df_columns:
                queryString += "{},'{}',".format(col,col_delimiter)

            queryString += "{} From mergeRowsView".format(df_columns[-1])
        else:
            queryString = "Select concat("
            for col in merging_cols:
                queryString += "{},'{}',".format(col,col_delimiter)

            queryString += "{} from mergeRowsView".format(merging_cols[-1])
        
        return spark.sql(queryString)

    except Exception as e:
        logger.error("Exception {} at columns concatination".format(e))

def add_columns(spark, df, new_col_def):

    try:
        for key in new_col_def:
            cur_def = new_col_def[key]
            if(cur_def["column_name"]=="dateTime"):
                if(cur_def["expression"]=="now"):
                    df = df.withColumns(cur_def["column_name"],datetime.datetime.now)
                elif(cur_def["expression"]=="date"):
                    df = df.withColumns(cur_def["column_name"],datetime.date)
            else:
                df = df.withColumns(cur_def["column_name"],cur_def["expression"])
        
        return df
    except Exception as e:
        logger.error("Exception {} at adding column to dataframe".format(e))

        return None