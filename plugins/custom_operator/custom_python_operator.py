from airflow.operators.python import PythonOperator

from typing import Sequence

class SQLTemplatedPythonOperator(PythonOperator):

    template_ext: Sequence[str] = (".sql",)
