import json
from airflow import DAG
from airflow.models import Variable
from datetime import timedelta
from airflow.operators.empty import EmptyOperator
from custom_operator.sql_to_file_postgres import SqlToFileOperator
from airflow.utils.trigger_rule import TriggerRule

ENV = Variable.get("env", deserialize_json=True)
CONFIG = {}
with open(ENV['dags_folder'] + "etl_sales_dashboard/raw.json", 'r') as f:
    CONFIG = json.load(f)

START_DATETIME = '{{ macros.ds_format(data_interval_start + macros.timedelta(hours=7), "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S") }}'
END_DATETIME = '{{ macros.ds_format(data_interval_end + macros.timedelta(hours=7), "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S") }}'

def create_dag(dag_id, version, tasks):
    default_args = {
        'owner': CONFIG['owner'],
        'depends_on_past': False,
        'retries': 1,
        'email': CONFIG['email_alert'],
        'retry_delay': timedelta(seconds=30),
        'start_date': version['start_date'],
        'provide_context': True,
        'priority_weight': CONFIG["priority_weight"]
    }

    if "end_date" in version and version["end_date"] is not None:
        default_args.update({"end_date": version["end_date"]})

    dag = DAG(
        dag_id,
        default_args=default_args,
        max_active_runs=5,
        schedule_interval=version['schedule_interval'],
        tags=version['tags'],
        template_searchpath=ENV['query_folder']
    )

    with dag:

        init = EmptyOperator(
            task_id="init"
        )


        generate_raw = SqlToFileOperator(
            task_id = tasks['raw']['id'],
            query = tasks["raw"]["query"],
            sql_conn_source=CONFIG['conn_source'],
            output_query="SQL_table",
            sql_conn_target=CONFIG['conn_target'],
            db_source='mysql',
            sql_table=tasks["raw"]["table"],
            sql_duplicate_key_handling="REPLACE",
            replace=True,
            email_on_failure=True,
            email_on_retry=False,
            cast_datatype= {"raw_mart_sales_dashboard_is_refund" : "bool"},
            clean_data_by_regex = r'[\r\n\t]+',
            dag = dag
        )


        end = EmptyOperator(
            task_id="end"
        )

        init >> generate_raw  >> end

    return dag


for version in CONFIG['versions']:
    dag_id = CONFIG['dag'] + "_" + version['name']
    globals()[dag_id] = create_dag(dag_id, version, CONFIG['tasks'])
