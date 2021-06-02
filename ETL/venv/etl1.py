from datetime import timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['rashnik.2020@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)}

dag = DAG(
    'tutorial',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example'])

t1 = BashOperator(
    task_id='extract_load',
    bash_command='/Users/rashmith/ETL/venv/extract_load.py',
    dag=dag
)

t2 = BashOperator(
    task_id='Transform',
    bash_command='/Users/rashmith/ETL/venv/transform.py',
    dag=dag
)

t1 >> t2
