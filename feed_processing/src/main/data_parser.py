from pyspark.sql import SparkSession
import sys
import os
import logging
import great_expectations as gex
import json
from validate import val_schema
from load import write_to_sink

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'../../configuration')))

from custom_logging import set_logging

set_logging()
logger = logging.getLogger('readfilelog')

class DataParser:
    def __init__(self, filename, file_path, infer_schema=False, header=False, df_name= None):
        self.__dataframe=None
        self._df_name = df_name
        self.filename = filename
        self.file_path = file_path
        self.infer_schema = infer_schema
        self.header = header
        self.bad_rows = None
        self.dq_conf = None
        self.data_model=None
    def get_df():
        return self.__dataframe

    # reading data from files
    # parse spark obj, path to read file, file extension
    def file_reader(spark):
        file_extension = self.file_path.split('.')[-1]
        if(file_extension=="json"):
            self.__dataframe = self.read_json(spark, self.file_path)
        elif(file_extension=="csv"):
            self.__dataframe = self.read_csv(spark, self.file_path,self.header,self.inferSchema)
        elif(file_extension=="txt"):
            self.__dataframe = self.read_text(spark, self.file_path)

    # read json file
    def read_json(spark, path):
        try:
            logger.info("reading input json file")
            self.__dataframe = spark\
                        .read\
                        .option("mode","PERMISSIVE")\
                        .option("nullValue",None)\
                        .json(path)
        except Exception as e:
            logger.error("exception {} while reading input json file".format(e))

    # read csv file
    def read_csv(spark, path,header,inferSchema):
        try:
            logger.info("reading input csv file")
            self.__dataframe = spark\
                        .read\
                        .option("header",header)\
                        .option("inferschema",inferSchema)\
                        .csv(path)
        except Exception as e:
            logger.error("exception {} while reading input csv file".format(e))

    # read text file
    def read_text(spark, path):
        try:
            logger.info("reading input text file")
            self.__dataframe = spark\
                        .read\
                        .text(path)
        except Exception as e:
            logger.error("exception {} while reading input text file".format(e))


    # def read_parquet()ss

    def data_validation():

        print(gex.__version__)

        # file data context
        context = gex.get_context(mode='file')
        print(context)

        # data sources
        data_source = context.data_sources.add_or_update_spark(name=self._df_name+"_source")
        # data assets
        data_asset = data_source.add_dataframe_asset(name=self._df_name+"_asset")
        # # batch definiton
        batch_definition = data_asset.add_batch_definition_whole_dataframe(self._df_name+"_definition")

        batch_parameters={"dataframe":self.__dataframe}
        batch = batch_definition.get_batch(batch_parameters=batch_parameters)
        # batch_request = gex.core.batch.RuntimeBatchRequest(
        #     datasource_name = "my_spark_datasource",
        #     data_connector_name = "runtime_data_connector",
        #     data_asset_name = df_name+"_asset",
        #     runtime_parameters = {"batch_data":df},
        #     batch_identifiers = {"batch_id":df_name+"_batch"}
        # )
        
        with open("/home/kumar/datapipeline-pyspark/feed_processing/configuration/data_quality/"+df_name+"_dq_config.json","r") as json_file:
            self.dq_conf = json.load(json_file)

        suite_name = self.dq_conf["expectation_suite_name"]
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
        expectations_len = len(self.dq_conf["expectations"])
        for i in range(0,expectations_len):
            args = self.dq_conf["expectations"][i]["args"]
            meta = self.dq_conf["expectations"][i]["meta"]
            if(self.dq_conf["expectations"][i]["expectation_name"]=="ExpectColumnValueLengthsToEqual"):
                expectation = gex.expectations.ExpectColumnValueLengthsToEqual(\
                    column=args["column"], value = args["value"], meta = meta
                )
                exp_suite.add_expectation(expectation)
            elif(self.dq_conf["expectations"][i]["expectation_name"]=="ExpectColumnValueLengthsToBeBetween"):
                expectation = gex.expectations.ExpectColumnValueLengthsToBeBetween(\
                    column=args["column"], min_value=args["min_value"],max_value=args["max_value"], meta = meta
                )
                exp_suite.add_expectation(expectation)
            elif(self.dq_conf["expectations"][i]["expectation_name"]=="ExpectColumnToExist"):
                expectation = gex.expectations.ExpectColumnToExist(
                    column=args["column"], meta = meta
                )
                exp_suite.add_expectation(expectation)
            elif(self.dq_conf["expectations"][i]["expectation_name"]=="ExpectColumnValuesToMatchRegexList"):
                expectation = gex.expectations.ExpectColumnValuesToMatchRegexList(
                    column=args["column"], regex_list = args["regex_list"], match_on = args["match_on"], meta = meta
                )
                exp_suite.add_expectation(expectation)
            elif(self.dq_conf["expectations"][i]["expectation_name"]=="ExpectColumnValuesToMatchRegex"):
                expectation = gex.expectations.ExpectColumnValuesToMatchRegex(
                    column=args["column"], regex = args["regex"], meta = meta
                )
                exp_suite.add_expectation(expectation)
            elif(self.dq_conf["expectations"][i]["expectation_name"]=="ExpectColumnValuesToBeUnique"):
                expectation = gex.expectations.ExpectColumnValuesToBeUnique(
                    column=args["column"], meta = meta
                )
                exp_suite.add_expectation(expectation)
            elif(self.dq_conf["expectations"][i]["expectation_name"]=="ExpectColumnValuesToBeOfType"):
                expectation = gex.expectations.ExpectColumnValuesToBeOfType(
                    column=args["column"], type_ = args["type"], meta = meta
                )
                exp_suite.add_expectation(expectation)
            elif(self.dq_conf["expectations"][i]["expectation_name"]=="ExpectColumnValuesToBeNull"):
                expectation = gex.expectations.ExpectColumnValuesToBeNull(
                    column=args["column"], meta = meta
                )
                exp_suite.add_expectation(expectation)
            elif(self.dq_conf["expectations"][i]["expectation_name"]=="ExpectColumnValuesToBeInSet"):
                expectation = gex.expectations.ExpectColumnValuesToBeInSet(
                    column=args["column"], value_set = args["value_set"], mostly = args["mostly"], meta = meta
                )
                exp_suite.add_expectation(expectation)
            elif(self.dq_conf["expectations"][i]["expectation_name"]=="ExpectColumnValuesToNotBeNull"):
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
        logger.info(validation_results)

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
                        if(self.bad_rows==None):
                            self.bad_rows = self.__dataframe.where(col(cur_exp_res["expectation_config"]["kwargs"]["column"])==exp_fail_keys[j])
                        else:
                            self.bad_rows = self.bad_rows.union(self.__dataframe.where(col(cur_exp_res["expectation_config"]["kwargs"]["column"])==exp_fail_keys[j]))
                        
                        # removing bad rows
                        self.__dataframe = self.__dataframe.where(col(cur_exp_res["expectation_config"]["kwargs"]["column"])!=exp_fail_keys[j])
        
        return True
        # expectation=gex.expecations.ExpectColumnValuesToBeBetween(
        #     column='col_name', max_value=max,min_value=min
        # )
        # validation_results=batch.validate(expectation)
    
    def schema_validation():

        try:

            with open("/home/kumar/datapipeline-pyspark/feed_processing/data_model/"+self.filename.lower()+"Model.json",'r') as json_file:
                self.data_model = json.load(json_file)

            if(self.get_df()!=None and self.data_model!=None):
                return val_schema(self.data_model, self.get_df())

            else:
                logger.warn("can not do schema validation, either df or data model is not parsed.")
                return False

        except Exception as e:
            logger.error("Exception {} at schema validation.".format(e))
            return False

    
    def write_data(write_flag,sink, sink_path,connection_uri, write_disposition, create_disposition, df=None):

        try:
            if(write_flag=="bad_rows_write"):
                logger.info("writing bad rows.")
                write_to_sink(df=df, sink=sink, sink_path=sink_path, connection_uri=connection_uri, write_disposition=write_disposition, create_disposition=create_disposition)

            else:
                logger.info("writing df rows.")
                write_to_sink(df=self.get_df(), sink=sink, sink_path=sink_path+df_name, connection_uri=connection_uri, write_disposition=write_disposition, create_disposition=create_disposition)
            

        except Exception as e:

            logger.error("Exception {} at writing to sink".format(e))
