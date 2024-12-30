from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, col
from py4j.java_gateway import java_import
import logging
import sys
import os
import great_expectations as gex
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'../../configuration')))

from custom_logging import set_logging

set_logging()
logger = logging.getLogger('validatelog')


def val_spark_obj(spark):
    
    try:

        logger.info("validated spark object: {}".format(spark.sql("SELECT current_date as date").collect()))
        return True

    except Exception as e:
        logger.error("exception {} while validating spark session obj".format(e))
        return False


def val_feed(spark, input_file_path):

    try:
        logger.info("validating input file {} at {}".format(input_file_path,current_date()))

        s_context = spark.sparkContext

        java_import(s_context._gateway.jvm,"org.apache.hadoop.fs.FileSystem")
        java_import(s_context._gateway.jvm,"org.apache.hadoop.fs.Path")

        hadoop_conf = s_context._jsc.hadoopConfiguration()
        print("done1")
        hdfs = s_context._gateway.jvm.FileSystem.get(hadoop_conf)
        print("done2",type(hdfs))

        path = s_context._gateway.jvm.Path(input_file_path)
        print("done3")
        file_status = hdfs.getFileStatus(path)
        print("done4",file_status)

        size = file_status.getLen()
        logger.info("file size is: {}".format(size))

        if(size==0):
            raise "input file size is 0B, couldn't process it."
            
        return True

    except Exception as e:
        logger.warn("exception {} while file validation, check input_file and its path".format(e))
        return False

# data validation

def val_schema(actualModel,df):

    try:
            
        logger.info("Inside schema validation for {}".format(df))
        # logger.info("printing schema {}".format(df.printSchema()))
        
        df_columns = df.columns
        actualModelDict = actualModel["schema"]
        model_keys = list(actualModelDict.keys())
        if(len(df_columns)==len(model_keys)):
            model_keys.sort()
            df_columns.sort()
            df_columns_types = df.dtypes
            for i in range(0,len(df_columns)):
                if(df_columns[i]!=model_keys[i]):

                    logger.warn("schema not match with actual df columns for {} model {} df particularly {} != {}".format(model_keys,df_columns,model_keys[i],df_columns[i]))
                    return False
                if(df_columns_types[i][1]!=actualModelDict[df_columns_types[i][0]]['type']):
                    logger.warn("type not match with actual df columns for {} model {} df {}--{} columns, {}--{}types".format(actualModel,df,model_keys[i],df_columns[i],df_columns_types[i][1],actualModelDict[df_columns_types[i][0]]['type']))
                    return False
        else:
            df.show()
            logger.warn("noof df columns not match with actual model columns for {} model {} df".format(actualModel,df))
            return False

    except Exception as e:
        logger.error("exception {} while schema validation".format(e))
        return False

    logger.info("successfully schema validation done")
    return True

'''
def val_data_cleaning(actualModel,df):

    try:

        logger.info("validating data for df {}".format(df))

        df.show()
        df_columns = df.columns
        actualModel_dict = actualModel["schema"]

        for column in df_columns:

            if(actualModel_dict[column]["required"]=="true"):
                col_nulls_from_df=df.where(col(column).isNull())
                col_nulls_from_df.show()

                if(col_nulls_from_df.count()!=0):
                    logger.warn("having null value for required field {}".format(column))
                    logger.warn("rows with null values for above field: {} \n removing rows with null".format(col_nulls_from_df.collect()))
                    df=df.where(col(column).isNotNull())

                else:
                    logger.info(f"No null values for required field {column}")
        return df

    except Exception as e:
        logger.error("Exception {} while data validation at checking null values for required fields.".format(e))
        return False'''

def data_validation(df,df_name):

    print(gex.__version__)

    # file data context
    context = gex.get_context(mode='file')
    print(context)

    # data sources
    data_source = context.data_sources.add_or_update_spark(name=df_name+"_source")
    # data assets
    data_asset = data_source.add_dataframe_asset(name=df_name+"_asset")
    # # batch definiton
    batch_definition = data_asset.add_batch_definition_whole_dataframe(df_name+"_definition")

    batch_parameters={"dataframe":df}
    batch = batch_definition.get_batch(batch_parameters=batch_parameters)
    # batch_request = gex.core.batch.RuntimeBatchRequest(
    #     datasource_name = "my_spark_datasource",
    #     data_connector_name = "runtime_data_connector",
    #     data_asset_name = df_name+"_asset",
    #     runtime_parameters = {"batch_data":df},
    #     batch_identifiers = {"batch_id":df_name+"_batch"}
    # )
    
    with open("/home/kumar/datapipeline-pyspark/feed_processing/configuration/data_quality/"+df_name+"_dq_config.json","r") as json_file:
        dq_conf = json.load(json_file)

    suite_name = dq_conf["expectation_suite_name"]
    suite = gex.ExpectationSuite(name=suite_name)
    suite_flag=True
    try:
        exp_suite = context.suites.get(name=suite_name)
    except Exception as e:
        suite_flag=False
        logger.warn("exception {} at get suite. create/add suite to context.".format(e))
    
    if(not suite_flag):
        suite = context.suites.add(suite)
        exp_suite = context.suites.get(name=suite_name)
    expectations_len = len(dq_conf["expectations"])
    for i in range(0,expectations_len):
        args = dq_conf["expectations"][i]["args"]
        meta = dq_conf["expectations"][i]["meta"]
        if(dq_conf["expectations"][i]["expectation_name"]=="ExpectColumnValueLengthsToEqual"):
            expectation = gex.expectations.ExpectColumnValueLengthsToEqual(\
                column=args["column"], value = args["value"], meta = meta
            )
            exp_suite.add_expectation(expectation)
        elif(dq_conf["expectations"][i]["expectation_name"]=="ExpectColumnValueLengthsToBeBetween"):
            expectation = gex.expectations.ExpectColumnValueLengthsToBeBetween(\
                column=args["column"], min_value=args["min_value"],max_value=args["max_value"], meta = meta
            )
            exp_suite.add_expectation(expectation)
        elif(dq_conf["expectations"][i]["expectation_name"]=="ExpectColumnToExist"):
            expectation = gex.expectations.ExpectColumnToExist(
                column=args["column"], meta = meta
            )
            exp_suite.add_expectation(expectation)
        elif(dq_conf["expectations"][i]["expectation_name"]=="ExpectColumnValuesToMatchRegexList"):
            expectation = gex.expectations.ExpectColumnValuesToMatchRegexList(
                column=args["column"], regex_list = args["regex_list"], match_on = args["match_on"], meta = meta
            )
            exp_suite.add_expectation(expectation)
        elif(dq_conf["expectations"][i]["expectation_name"]=="ExpectColumnValuesToMatchRegex"):
            expectation = gex.expectations.ExpectColumnValuesToMatchRegex(
                column=args["column"], regex = args["regex"], meta = meta
            )
            exp_suite.add_expectation(expectation)
        elif(dq_conf["expectations"][i]["expectation_name"]=="ExpectColumnValuesToBeUnique"):
            expectation = gex.expectations.ExpectColumnValuesToBeUnique(
                column=args["column"], meta = meta
            )
            exp_suite.add_expectation(expectation)
        elif(dq_conf["expectations"][i]["expectation_name"]=="ExpectColumnValuesToBeOfType"):
            expectation = gex.expectations.ExpectColumnValuesToBeOfType(
                column=args["column"], type_ = args["type"], meta = meta
            )
            exp_suite.add_expectation(expectation)
        elif(dq_conf["expectations"][i]["expectation_name"]=="ExpectColumnValuesToBeNull"):
            expectation = gex.expectations.ExpectColumnValuesToBeNull(
                column=args["column"], meta = meta
            )
            exp_suite.add_expectation(expectation)
        elif(dq_conf["expectations"][i]["expectation_name"]=="ExpectColumnValuesToBeInSet"):
            expectation = gex.expectations.ExpectColumnValuesToBeInSet(
                column=args["column"], value_set = args["value_set"], mostly = args["mostly"], meta = meta
            )
            exp_suite.add_expectation(expectation)
        elif(dq_conf["expectations"][i]["expectation_name"]=="ExpectColumnValuesToNotBeNull"):
            expectation = gex.expectations.ExpectColumnValuesToNotBeNull(
                column=args["column"], meta = meta
            )
            exp_suite.add_expectation(expectation)
        
    # validation_definition = gex.ValidationDefinition(
    #     data=batch_definition,
    #     suite=exp_suite,
    #     name = df_name+"_validation_definition"
    # )
    # validation_definition = context.validation_definitions.get(df_name+"_validation_definition")
    validator = context.get_validator(
        batch=batch,
        expectation_suite_name=suite_name
    )
    validation_results = validator.validate()
    print(validation_results)

    if(validation_results["success"]==False):
        results_len = len(validation_results["results"])
        for i in range(0,results_len):
            cur_exp_res = validation_results["results"][i]
            if(cur_exp_res["success"]==False and (cur_exp_res["expectation_config"]["meta"]["discard_column_on_failure"] or cur_exp_res["expectation_config"]["meta"]["discard_row_on_failure"])):
                logger.warn("expectation failure for expectation {} on column {}".format(cur_exp_res["expectation_config"]["type"],cur_exp_res["expectation_config"]["kwargs"]["column"]))
                logger.info("removing col/row as dq config...")
                exp_fail_keys = cur_exp_res["result"]["partial_unexpected_list"]
                exp_fail_keys_len = len(exp_fail_keys)
                for j in range(0,exp_fail_keys_len):
                    df = df.where(col(cur_exp_res["expectation_config"]["kwargs"]["column"])!=exp_fail_keys[j])
    return df

    # expectation=gex.expecations.ExpectColumnValuesToBeBetween(
    #     column='col_name', max_value=max,min_value=min
    # )
    # validation_results=batch.validate(expectation)
