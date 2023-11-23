from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime
import logging

postgres_conn_id = "PG_WAREHOUSE_CONNECTION"
table_cdn_plans = "cdn.plans"
table_cdn_editors = "cdn.editors"
table_dds_plans = "dds.plans"
table_dds_editors = "dds.editors"


def sql_exec(sql):
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    hook.run(sql)


def create_cdn_plans_with_doubles():
    sql = """
    drop table if exists cdn.plans_with_doubles;
    create table cdn.plans_with_doubles as (
        select 
            *
        from 
            dds.plans
        order by
            source_id, 
            entry_date desc
    );
    """
    sql_exec(sql)

def create_cdn_plans_actual():
    sql = '''
    drop table if exists cdn.plans_actual;
    create table cdn.plans_actual as (
        select
            distinct on(source_id) source_id,
            educational_profile ,
            number ,
            approval_date ,
            year ,
            education_form ,
            qualification ,
            on_check ,
            laboriousness ,
            rating ,
            entry_date ,
            is_deleted,
            name_op ,
            code_op ,
            faculty_op
        from
            dds.plans
        order by
            source_id,
            entry_date desc
    );
    '''
    sql_exec(sql)

def create_cdn_editors():
    sql = """
    drop table if exists cdn.editors;
    create table cdn.editors as (
        select 
            distinct on(source_id, id_editor)
            source_id,
            id_editor ,
            user_name ,
            first_name ,
            last_name ,
            email ,
            isu_number ,
            entry_date
        from 
            dds.editors
        order by
            source_id,
            id_editor,
            entry_date desc
            
    );
    """
    sql_exec(sql)


def create_cdn_plans_with_editors():
    sql = '''
    drop table if exists cdn.plans;
    drop table if exists cdn.plans_with_editors;
    create table cdn.plans_with_editors as (
    select
        distinct
        t_1.source_id as editor_source_id,
        t_1.id_editor,
        t_1.user_name,
        t_1.first_name,
        t_1.last_name,
        t_1.email,
        t_1.isu_number,
        t_2.year,
        t_2.qualification,
        t_2.is_deleted,
        t_2.name_op,
        t_2.code_op,
        t_2.faculty_op,
        t_2.laboriousness,
        t_2.on_check,
        t_2.entry_date
    from
        cdn.editors t_1
    left join lateral (
        select *
        from cdn.plans t
        where t_1.source_id = t.source_id
        order by t.entry_date desc
        limit 1
    ) t_2 on true
    order by
        t_1.source_id, t_2.entry_date desc
    );
    '''
    sql_exec(sql)

dag = DAG(
    'dds_to_cdn',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
)

execute_sql_task = PythonOperator(
    task_id='create_cdn_plans_with_doubles',
    python_callable=create_cdn_plans_with_doubles,
    dag=dag,
)

execute_sql_task = PythonOperator(
    task_id='create_cdn_plans_actual',
    python_callable=create_cdn_plans_actual,
    dag=dag,
)

execute_sql_task = PythonOperator(
    task_id='create_cdn_editors',
    python_callable=create_cdn_editors,
    dag=dag,
)

execute_sql_task = PythonOperator(
    task_id='create_cdn_plans_with_editors',
    python_callable=create_cdn_plans_with_editors,
    dag=dag,
)