from datetime import datetime, timedelta
import json
import os

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = dict(
    owner='chernov',
    retries=3,
    retry_delay=timedelta(minutes=1)
)


def extract_data():
    f = open(os.getenv('DATA_DIR') + '/data/staging/v1/plans.json', 'r')
    old_plans = json.loads(f.read())
    print(old_plans)
    
    # f = open(os.getenv('DATA_DIR') + '/v1/modules.json', "r")
    # old_modules = json.loads(f.read())


def transform_data():
    pass


def load_data():
    pass


with DAG(
    dag_id='archive_data',
    default_args=default_args,
    description="Get archived data",
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    ext_data = PythonOperator(task_id='extract_archive_data',
                           python_callable=extract_data)
    ext_data
