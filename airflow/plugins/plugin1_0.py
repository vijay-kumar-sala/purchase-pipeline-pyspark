from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator



def run_spark_job(**context):

    spark_master=context["master"]
    spark_mongo_package=context["packages"]
    main=context["main"]
    app_name=context["app_name"]
    input_path=context["input_path"]
    sink=context["sink"]
    output_path=context["output_path"]
    connection_uri=context["connection_uri"]
    write_disposition=context["write_disposition"]
    create_disposition=context["create_disposition"]

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
            "--output_path",output_path,
            "--connection_uri",connection_uri,
            "--write_disposition",write_disposition,
            "--create_disposition",create_disposition
        ]
    )

    submit_spark.execute(context)
    return ['end']
