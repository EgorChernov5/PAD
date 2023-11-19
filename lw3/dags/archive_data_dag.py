from datetime import datetime, timedelta
import json

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = dict(
    owner='chernov',
    retries=3,
    retry_delay=timedelta(minutes=1)
)


def etl_data():
    print("MY LOG: Extract archive data...")
    f = open("./data/staging/archive_data/modules.json", 'r')
    modules = json.loads(f.read())
    
    f = open("./data/staging/archive_data/plans.json", 'r')
    plans = json.loads(f.read())
    print("MY LOG: Complite!")

    print("MY LOG: Transform archive data...")
    for plan in plans['plans']:
        plan['is_deleted'] = False
    
    for module in modules['modules']:
        module['is_deleted'] = False
    print("MY LOG: Complite!")

    print("MY LOG: Load archive data...")
    with open("./data/staging/staging_plans.json", 'w') as file:
        json.dump(plans, file)

    with open("./data/staging/staging_modules.json", 'w') as file:
        json.dump(modules, file)
    print("MY LOG: Complite!")


with DAG(
    dag_id='archive_data',
    default_args=default_args,
    description="Get archived data",
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    task = PythonOperator(task_id='etl_archive_data',
                           python_callable=etl_data)
    task
