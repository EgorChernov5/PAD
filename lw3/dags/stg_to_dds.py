from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import json
import pandas as pd
import logging

postgres_conn_id = "PG_WAREHOUSE_CONNECTION"
table_name = "dds.plans"
table_name_editors = "dds.editors"


def get_plans_df():
    file_path = "./data/staging/staging_plans.json"
    with open(file_path, 'r') as file:
        plans = json.load(file)

    rows = []
    for plan in plans.get('plans', []):
        academic_plan = plan.get("academic_plan_in_field_of_study", [{}])[0]

        try:
            year = int(academic_plan.get("year"))
        except (ValueError, TypeError):
            year = None

        try:
            code_op = str(academic_plan.get("field_of_study", [{}])[0].get("number"))
        except (KeyError, IndexError, TypeError):
            code_op = None

        try:
            faculty_op = str(academic_plan["structural_unit"]["title"])
        except:
            faculty_op = None

        row = {
            'source_id': plan.get('id'),
            'educational_profile': plan.get('educational_profile'),
            'number': int(plan.get('number')) if plan.get('number') else None,
            'approval_date': plan.get('approval_date'),
            'year': year,
            'education_form': plan.get('education_form'),
            'qualification': academic_plan.get('qualification'),
            'on_check': plan.get('on_check'),
            'laboriousness': int(plan.get('laboriousness')),
            'rating': bool(plan.get('rating')),
            'entry_date': plan.get('entry_date'),
            'is_deleted': bool(plan.get("is_deleted")),
            'name_op': str(academic_plan.get("title")),
            'code_op': code_op,
            'faculty_op': faculty_op
        }
        rows.append(row)

    df_plans = pd.DataFrame(rows)
    df_plans['entry_date'] = pd.to_datetime(df_plans['entry_date'])
    return df_plans


def get_editors_df():
    file_path = "./data/staging/staging_plans.json"
    with open(file_path, 'r') as file:
        plans = json.load(file)

    rows = []
    for plan in plans.get('plans', []):
        academic_plan = plan.get("academic_plan_in_field_of_study", [{}])[0]
        try:
            for editor in academic_plan["editors"]:
                row = {
                    "source_id": plan.get('id'),
                    "id_editor": editor["id"],
                    "user_name": editor["username"],
                    "first_name": editor["first_name"],
                    "last_name": editor["last_name"],
                    "email": editor["email"],
                    "isu_number": editor["isu_number"],
                    'entry_date': plan.get('entry_date')
                }
                rows.append(row)
        except Exception as e:
            print(f"Error processing plan: {plan}. Error: {e}")

    df_editors = pd.DataFrame(rows)
    df_editors['entry_date'] = pd.to_datetime(df_editors['entry_date'])
    return df_editors


def plans():
    try:
        hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        hook.run(f"""
            drop table if exists {table_name};
            create table {table_name} (
            source_id INT,
            educational_profile VARCHAR(255), 
            number INT,
            approval_date DATE,
            year INT,
            education_form VARCHAR(255), 
            qualification VARCHAR(255),
            on_check VARCHAR(255),
            laboriousness INT,
            rating BOOLEAN,
            entry_date TIMESTAMP,
            is_deleted BOOLEAN,
            name_op VARCHAR(255),
            code_op VARCHAR(255),
            faculty_op  VARCHAR(255)
            );
        """)

        plans_df = get_plans_df()
        columns_order_df = hook.get_pandas_df(f"SELECT * from {table_name} LIMIT 1;")
        target = [i for i in columns_order_df.columns.values if i in plans_df.columns.values]
        hook.insert_rows(table_name, plans_df[target].values, target_fields=target)

    except Exception as e:
        logging.error("Ошибка во время вставки планов: %s", str(e))
        raise  # повторное возбуждение исключения для пометки задачи как неудавшейся


def editors():
    try:
        hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        hook.run(f"""
            drop table if exists {table_name_editors};
            create table {table_name_editors} (
            source_id INT,
            id_editor INT,
            user_name VARCHAR(255),
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            email VARCHAR(255),
            isu_number INT,
            entry_date TIMESTAMP
            );
        """)

        editors_df = get_editors_df()
        columns_order_df = hook.get_pandas_df(f"SELECT * from {table_name_editors} LIMIT 1;")
        target = [i for i in columns_order_df.columns if i in editors_df.columns]
        hook.insert_rows(table_name_editors, editors_df[target].values, target_fields=target)
    except Exception as e:
        logging.error("Ошибка во время вставки редакторов: %s", str(e))
        raise  # повторное возбуждение исключения для пометки задачи как неудавшейся


def get_disciplines_df():
    file_path = "./data/staging/staging_modules.json"
    with open(file_path, 'r') as file:
        modules = json.load(file)
    modules_df = pd.DataFrame.from_dict(modules["modules"])
    columns = ["id", "approval_date", "authors", "discipline_code", "title", "qualification", "wp_status"]

    def get_wp(x):
        result = []
        try:
            for i in x:
                item = i["work_program"][0]
                temp = {}
                for column in columns:
                    temp[column] = item.get(column, None)
                result.append(temp)
            return result
        except:
            return []

    def valid_check(x):
        try:
            x["id"]
            return True
        except:
            return False

    modules_df = modules_df[["id", "name", "change_blocks_of_work_programs_in_modules", "entry_date"]]
    modules_df = modules_df.rename(columns={"id": "module_id","name":"module_name", "change_blocks_of_work_programs_in_modules": "wp_object", "entry_date": "module_entry_date"})
    modules_df["wp_object"] = modules_df["wp_object"].apply(get_wp)
    modules_df = modules_df.reset_index(drop=True)
    modules_df = modules_df.explode("wp_object").reset_index(drop=True)
    modules_df["valid"] = modules_df.wp_object.apply(valid_check)
    modules_df = modules_df[modules_df["valid"]==True]
    modules_df = modules_df.reset_index(drop=True)

    for column in columns:
        modules_df[column] = modules_df["wp_object"].apply(lambda x: x[column])
    modules_df = modules_df.drop(["wp_object", "valid"], axis=1).rename(columns={"id": "source_id"})
    modules_df['module_entry_date'] = pd.to_datetime(modules_df['module_entry_date'])
    modules_df['approval_date'] = pd.to_datetime(modules_df['approval_date'])
    return modules_df


def disciplines():
    df = get_disciplines_df()
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    hook.run(f"""
        DROP TABLE IF EXISTS dds.disciplines;
        
        CREATE TABLE IF NOT EXISTS dds.disciplines
        (
            id integer NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
            module_id integer,
            module_name text,
            module_entry_date TIMESTAMP,
            source_id integer,
            approval_date TIMESTAMP,
            authors text,
            discipline_code integer,
            title text,
            qualification text,
            wp_status text,
            CONSTRAINT disciplines_pkey PRIMARY KEY (id)
        )
    """)

    columns_order_df = hook.get_pandas_df(f"SELECT * from dds.disciplines LIMIT 1;")
    target = [i for i in columns_order_df.columns.values if i in df.columns.values]
    hook.insert_rows("dds.disciplines", df[target].values, target_fields=target)

    
    

dag = DAG(
    '_stg_to_dds_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
)


execute_sql_task = PythonOperator(
    task_id='plans_task',
    python_callable=plans,
    dag=dag,
)

execute_sql_task = PythonOperator(
    task_id='editors_task',
    python_callable=editors,
    dag=dag,
)

execute_sql_task = PythonOperator(
    task_id='disciplines_task',
    python_callable=disciplines,
    dag=dag,
)