from pyspark.sql import SparkSession
# from pyspark.context import SparkContext
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import lit
import logging
import sys
import os
import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'../../configuration')))

from custom_logging import set_logging

set_logging()
logger = logging.getLogger('transformlog')


def Combine(spark, df, keys, aggs):

    try:
        if(keys!=None and aggs!=None):

            # queryString = "SELECT {},".format(key)

            # if(sum!=None):
            #     queryString += format_query_str_agg_funs(sum.split(','),"sum")
            # if(avg!=None):
            #     queryString += format_query_str_agg_funs(avg.split(','),"avg")
            # if(min!=None):
            #     queryString += format_query_str_agg_funs(min.split(','),"min")
            # if(max!=None):
            #     queryString += format_query_str_agg_funs(max.split(','),"max")
            # if(count!=None):
            #     queryString += format_query_str_agg_funs(count.split(','),"count")
            
            # queryString = queryString[:-1] + ' '
            # queryString += "FROM tempView GROUP BY {}".format(key)

            # logger.warning("Printing query string: {}".format(queryString))
            # df.createOrReplaceTempView("tempView")
            # grouped_df = spark.sql(queryString)
            cols_in_aggs = aggs.keys()
            cols_in_df = df.columns
            for i in cols_in_aggs:
                if(i not in cols_in_df):
                    logger.warning("given aggregation columns not matched with df columns. for instance check col {}".format(i))
                    return None
                if(aggs[i] not in ["sum","avg","min","max","count"]):
                    logger.warning(f"unsupported aggregation definition {i}")
                    return None
            for i in keys:
                if(i not in cols_in_df):
                    logger.warning("given keys for grouping not matched with df columns. for instance key {}".format(i))
                    return None
            df = df.groupBy(keys).agg(aggs)
            for key in aggs:
                df = df.withColumnRenamed(f"{aggs[key]}({key})",key)
            df.show()
            return df

    except Exception as e:

        logger.error("exception {} while grouping and aggregation on df".format(e))

def join_df(spark, left_view, left_df, right_view ,right_df, Joining_condition, joining_type):

    try:
        if(Joining_condition!=None and joining_type!=None):
            left_df.show()
            right_df.show()
            if(isinstance(Joining_condition,str) and '=' in Joining_condition):
                left_df.createOrReplaceTempView(left_view)
                right_df.createOrReplaceTempView(right_view)
                queryString = "SELECT {}.*, {}.* FROM {} {} JOIN {} ON {}".format(left_view, right_view,left_view,joining_type, right_view,Joining_condition)
                print("query str:"+queryString)
                return spark.sql(queryString)

            return left_df.join(right_df,Joining_condition, joining_type)

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
                queryString += "coalesce({},'null'),'{}',".format(col,col_delimiter)
        else:
            queryString = "Select concat("
            for i in range(0, len(merging_cols)-1):
                queryString += "coalesce({},'null'),'{}',".format(merging_cols[i],col_delimiter)

        queryString = queryString[0:-5]
        queryString += ") as bad_row from mergeRowsView"
        
        return spark.sql(queryString)

    except Exception as e:
        logger.error("Exception {} at columns concatination".format(e))

def add_columns(spark, df, new_col_def):

    try:
        for key in new_col_def:
            cur_def = new_col_def[key]
            print(key)
            print(cur_def)
            if(cur_def["column_name"]=="dateTime"):
                if(cur_def["expression"]=="now"):
                    df = df.withColumn(cur_def["column_name"],lit(datetime.datetime.now()))
                    print("in datetime now")
                elif(cur_def["expression"]=="date"):
                    print("int datetime date")
                    df = df.withColumn(cur_def["column_name"],lit(datetime.datetime.now().strftime("%x")))
            else:
                print("in else")
                if(type(cur_def["expression"])==type("")):
                    df = df.withColumn(cur_def["column_name"],lit(cur_def["expression"]))
                else:
                    print("adding col with this.df cols expressions")
                    df = df.withColumn(cur_def["column_name"], cur_def["expression"])
        
        return df
    except Exception as e:
        logger.error("Exception {} at adding column to dataframe".format(e))

        return None

def groupByKey(spark, df, keys):

    try:
        if(keys!=None):
            df_columns = df.columns
            for i in keys:
                if(i not in df_columns):
                    logger.warning("given keys for groping not matching with df columns for key {}".format(i))
                    return None

            df = df.groupBy(keys)
            df.show()
            return df
        logger.info("groupByKey done")
    except Exception as e:
        logger.error("Exception {} at grouping the df by keys {}".format(e,keys))
        return 

def coGroupByKey(spark, dfs_list, keys):
    try:
        if(keys!=None):
            for df in dfs_list:
                df_cols = df.columns
                for key in keys:
                    if(key not in df_cols):
                        logger.warning(f"given keys not matching with df columns. for key {key}")
                        return 
            dfs_list_len = len(dfs_list)
            if(dfs_list_len>1):
                cur_df = dfs_list[0]
                for i in range(1,dfs_list_len):
                    cur_df = join_df(spark, cur_df, dfs_list[i], keys, "full")

            coGroupedDF = cur_df.groupBy(keys)
            coGroupedDF.show()
        return coGroupedDF
    except Exception as e:
        logger.error("Exception {} at cogroupbykey dfs".format(e))
        return

def flatten(spark, A_df, B_df):
    try:
        A_df.printSchema()
        B_df.printSchema()
        A_df_types = A_df.dtypes
        B_df_types = B_df.dtypes
        if(len(A_df_types)!=len(B_df_types)):
            logger.warning("Both dfs columns lengths not matched!")
            return
        A_df_types_len = len(A_df_types)
        for i in range(0,A_df_types_len):
            a_type = A_df_types[i][1]
            b_type = B_df_types[i][1]
            if(a_type != b_type):
                logger.warning("Both dfs columns types not matched!")
                return
        logger.info("Both dfs columns length matched and types matched.")
        return A_df.union(B_df)
    except Exception as e:
        logger.error("Exception {} at union of two dfs.".format(e))
        return