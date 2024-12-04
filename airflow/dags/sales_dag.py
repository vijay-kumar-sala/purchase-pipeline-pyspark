import dagfactory

path = "/home/kumar/airflow/dags/sales_dag.yaml"

dagfactory_obj = dagfactory.DagFactory(path)
dagfactory_obj.clean_dags(globals())
dagfactory_obj.generate_dags(globals())