import json
from airflow import DAG
from airflow.models import Variable
from datetime import timedelta
from airflow.macros import ds_format, ds_add
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from custom_operator.mysql_to_postgres import MySqlToPostgresOperator
from airflow.utils.trigger_rule import TriggerRule
from custom_utils.space_alert_notif import space_fail_alert

env = Variable.get("env", deserialize_json=True)
file_config = open(env['dags_folder'] + "etl_compliment/config.json", 'r')
config = json.load(file_config)
file_config.close()

def create_dag(dag_id, version):
    default_args = {
        'owner': config['owner'],
        'depends_on_past': True,
        'retries': 1,
        'email': config['email_alert'],
        'retry_delay': timedelta(seconds=30),
        'start_date': version['start_date'],
        'provide_context': True,
        'priority_weight': config["priority_weight"]
    }

    if "end_date" in version and version["end_date"] is not None:
        default_args.update({"end_date": version["end_date"]})

    dag = DAG(
        dag_id,
        default_args=default_args,
        max_active_runs=1,
        schedule_interval=version['schedule_interval'],
        tags=version['tags'],
        template_searchpath=env['query_folder'],
        on_failure_callback=space_fail_alert
    )

    with dag:

        init = EmptyOperator(
            task_id="init"
        )

        extract_load_marts = []
        for task in version["raw_tasks"]:
            extract_load_mart = MySqlToPostgresOperator(
                task_id=task["name"],
                query=task["extract_query"],
                mysql_conn=config['conn_source'],
                postgres_conn=config['conn_target'],
                target_table=task["table_target"],
                identifier="raw_mart_compliment_transaction_id",
                replace=True,
                db_query_from='mysql',
                email_on_failure=True,
                email_on_retry=False
            )
            extract_load_marts.append(extract_load_mart)

        end = EmptyOperator(
            task_id="end"
        )

        init >> extract_load_marts >> end

    return dag


for version in config['versions']:
    dag_id = config['dag'] + "_" + version['name']
    globals()[dag_id] = create_dag(dag_id, version)
