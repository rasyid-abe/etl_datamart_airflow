from airflow.hooks.base import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

SLACK_CONN_ID = 'slack-alert'

def slack_fail_alert(context):

    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    channel = BaseHook.get_connection(SLACK_CONN_ID).login
    slack_msg = f"""
            :x: Task Failed.
            *Task*: {context.get('task_instance').task_id}
            *Dag*: {context.get('task_instance').dag_id}
            *Execution Time*: {context.get('execution_date')}
        """

    slack_alert = SlackWebhookOperator(
        task_id='slack_alert',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        channel=channel,
        username=channel,
        http_conn_id=SLACK_CONN_ID
    )

    return slack_alert.execute(context=context)
