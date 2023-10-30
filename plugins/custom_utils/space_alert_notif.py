from airflow.hooks.base import BaseHook
from airflow.providers.http.operators.http import SimpleHttpOperator

SPACE_CONN_ID = 'space-alert'


def space_fail_alert(context):
    space_hook = BaseHook.get_connection(SPACE_CONN_ID)
    space_host = space_hook.host

    # error_message = context.get('task_instance').xcom_pull(task_ids=context.get('task_instance').task_id, key='error')

    log_msg = f"""
    *DAG Failed*
    *Dag* = {context.get('task_instance').dag_id}
    *Task* = {context.get('task_instance').task_id}
    *Execution Time* = {context.get('execution_date')}
    *Log* = {context.get('task_instance').log_url}
    """
    space_msg = {"text": log_msg}

    space_alert = SimpleHttpOperator(
        task_id='space_alert',
        http_conn_id=SPACE_CONN_ID,
        endpoint=space_host,
        method='POST',
        data=str(space_msg)
    )

    return space_alert.execute(context=context)
