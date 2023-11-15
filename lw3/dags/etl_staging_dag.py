from datetime import datetime, timedelta

from airflow import DAG


default_args = dict(
    owner='chernov',
    retries=3,
    retry_delay=timedelta(minutes=1)
)


def extract_data():
    pass


def transform_data():
    pass


def load_data():
    pass


with DAG(
    dag_id='etl_staging',
    default_args=default_args,
    description="Perform ETL for the staging layer",
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    pass
