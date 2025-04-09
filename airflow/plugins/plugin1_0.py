from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models.xcom import XCom


def run_spark_job_feed_proc(**context):
    task_instance=context["ti"]
    spark_master=context["master"]
    spark_mongo_package=context["packages"]
    main=context["main"]
    app_name=context["app_name"]
    input_path=context["input_path"]
    sink=context["sink"]
    staging_area=context["staging_area"]
    output_path=context["output_path"]
    connection_uri=context["connection_uri"]
    write_disposition=context["write_disposition"]
    create_disposition=context["create_disposition"]
    bad_rows_collection=context["bad_rows_collection"]

    if(context["is_xcom_push"]):
        for v in context["x_com_push_var"]:
            if(v in context):
                task_instance.xcom_push(key=v, value=context[v])

    submit_spark=SparkSubmitOperator(
        task_id="submit_spark",
        conn_id=spark_master,
        application=main,
        name=app_name,
        packages=spark_mongo_package,
        application_args=[
            "--app_name",app_name,
            "--input_path",input_path,
            "--sink",sink,
            "--staging_area",staging_area,
            "--output_path",output_path,
            "--connection_uri",connection_uri,
            "--write_disposition",write_disposition,
            "--create_disposition",create_disposition,
            "--bad_rows_collection",bad_rows_collection
        ]
    )
    submit_spark.execute(context)
    return ['main_loading']

def run_spark_job_main_loading(**context):
    task_instance = context["ti"]
    spark_master=context["master"]
    main=context["main"]
    app_name=context["app_name"]
    target_load_config = context["target_load_config"]
    xcom_pull_list=[]
    connection_uri=None
    packages = None
    if(context["is_xcom_pull"]):
        for v in context["x_com_pull_var"]:
            if(v not in context):
                if(v=="connection_uri"):
                    connection_uri_obj=task_instance.xcom_pull(key=v,task_ids=["feed_processing"])
                    print('xcom_pull return value')
                    print(connection_uri_obj)
                    connection_uri=connection_uri_obj.__getitem__(0)
                    print("-------------")
                elif(v=='packages'):
                    packages_obj=task_instance.xcom_pull(key=v,task_ids=["feed_processing"])
                    print('xcom_pull return value')
                    print(packages_obj)
                    packages=packages_obj.__getitem__(0)
                else:
                    xcom_pull_list.append({v, task_instance.xcom_pull(key=v,task_ids=["feed_processing"]).__getitem__(0)})
    # for kv in xcom_pull_list:
    #     print( kv)
    #     if(kv.key() == "connection_uri"):
    #         connection_uri = kv.get(kv.key())
    #     if(kv.key() == "packages"):
    #         packages = kv.get(kv.key())
    spark_submit_task = SparkSubmitOperator(
        task_id = "spark_submit_task",
        conn_id = spark_master,
        application = main,
        name = app_name,
        packages = packages,
        application_args=[
            "--app_name",app_name,
            "--target_load_config",target_load_config,
            "--connection_uri",connection_uri
        ]
    )
    spark_submit_task.execute(context)
    return ['end']