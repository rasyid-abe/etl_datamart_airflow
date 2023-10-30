import json
from airflow import DAG
from airflow.models import Variable
from datetime import timedelta
from airflow.operators.empty import EmptyOperator
from custom_operator.sql_to_file_postgres import SqlToFileOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.sensors.external_task import ExternalTaskSensor

ENV = Variable.get("env", deserialize_json=True)
# ID_STORES = Variable.get("id_store_cashier_beta", deserialize_json = False, default_var=None)
ID_STORES = None
CONFIG = {}
with open(ENV['dags_folder'] + "etl_tax_report/monthly.json", 'r') as f:
    CONFIG = json.load(f)

START_DATETIME = '{{ macros.ds_format(data_interval_start + macros.timedelta(hours=7), "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S") }}'
END_DATETIME = '{{ macros.ds_format(data_interval_end + macros.timedelta(hours=7), "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S") }}'

def create_dag(dag_id, version, tasks):
    default_args = {
        'owner': CONFIG['owner'],
        'depends_on_past': True,
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
        max_active_runs=1,
        schedule_interval=version['schedule_interval'],
        tags=version['tags'],
        template_searchpath=ENV['query_folder']
    )

    with dag:

        init = EmptyOperator(
            task_id="init"
        )


        # external_sensor = [
        #     ExternalTaskSensor(
        #         task_id = task['id'],
        #         external_dag_id=f"{task['dag']}_{version['name']}",
        #         external_task_id=task['task_id'],
        #         check_existence = True,
        #         mode="reschedule",
        #         poke_interval=60 * 15,
        #     ) for task in tasks["wait"]
        # ]

        load_extract_marts = [
            SqlToFileOperator(
                task_id = task['id'],
                query = task["query"],
                sql_conn_source=CONFIG['conn_source'],
                output_query="SQL_table",
                sql_conn_target=CONFIG['conn_target'],
                db_source='postgres',
                sql_table=task["table"],
                sql_duplicate_key_handling="REPLACE",
                replace=True,
                email_on_failure=True,
                email_on_retry=False,
                clean_data_by_regex = r'[\r\n\t]+',
                dag = dag
            ) for task in tasks['monthly']
        ]


        join = EmptyOperator(
            task_id="merge_mart_schedule",
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
        )

        wait_join = EmptyOperator(
            task_id="merge_wait_schedule",
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
        )

        end = EmptyOperator(
            task_id="end"
        )

        init >> wait_join  >> load_extract_marts  >> join >> end

    return dag


for version in CONFIG['versions']:
    dag_id = CONFIG['dag'] + "_" + version['name']
    globals()[dag_id] = create_dag(dag_id, version, CONFIG['tasks'])
