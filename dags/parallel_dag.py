import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

DAG_ID = os.path.basename(__file__).replace('.py', '')

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

dag_args = {
    "dag_id": DAG_ID,
    "description": 'Run built-in Spark app on Amazon EMR',
    "default_args": DEFAULT_ARGS,
    "dagrun_timeout": timedelta(hours=2),
    "start_date": datetime(2020, 1, 1),
    "schedule_interval": '@once',
    "tags": ['emr'],
}

with DAG(**dag_args) as dag:
    task_1 = BashOperator(
        task_id='task_1',
        bash_command='sleep 3'
    )
    with TaskGroup('processing_tasks') as parallel_task:
        task_2 = BashOperator(
            task_id='task_2', bash_command='sleep 3'
        )
        task_3 = BashOperator(
            task_id='task_3',
            bash_command='sleep 3'
        )

    task_4 = BashOperator(
        task_id='task_4',
        bash_command='sleep 3'
    )

    task_1 >> parallel_task >> task_4
