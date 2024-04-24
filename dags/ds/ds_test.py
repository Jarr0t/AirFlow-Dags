"""
Test DAG
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'Vladislav',
    'poke_interval': 600
}

with DAG('ds_test',
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['Educational']) as dag:

    empty = EmptyOperator(task_id='empty')

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def hello_world_call():
        logging.info('Hello World.')

    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_call,
        dag=dag
    )

    empty >> [echo_ds, hello_world]