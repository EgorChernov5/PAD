import numpy as np
import requests
import json
from datetime import datetime, timedelta
# import time

from airflow import DAG
from airflow.operators.python import PythonOperator


url = "https://op.itmo.ru/auth/token/login"
auth_data = {"username": "analytic", "password": "datatest"}
token_txt = requests.post(url, auth_data).text
token = json.loads(token_txt)["auth_token"]
headers = {'Content-Type': "application/json", 'Authorization': "Token " + token}

default_args = dict(
    owner='chernov',
    retries=3,
    retry_delay=timedelta(minutes=1)
)


def omit(obj, excluded_key):
    return {key: value for key, value in obj.items() if key != excluded_key}


def etl_data():
    # t = time.time()
    print("MY LOG: Extract staging data...")
    f = open("./data/staging/staging_plans.json", 'r')
    plans = json.loads(f.read())
    plan_ids = np.unique([plan['id'] for plan in plans['plans']])
    
    f = open("./data/staging/staging_modules.json", 'r')
    modules = json.loads(f.read())
    module_ids = np.unique([module['id'] for module in modules['modules']])
    print("MY LOG: Complite!")

    print("MY LOG: Extract data ids...")
    url = "http://op.itmo.ru/api/record/academic_plan/academic_wp_description/all"
    new_plan_ids = []
    next_url = url
    while True:    
        page = requests.get(next_url, headers=headers).json()
        next_url = page['next']    
        for plan in page['results']:
            new_plan_ids += [int(plan['id'])]

        if not next_url:
            break
    
    if requests.get(url, headers=headers).json()['count'] == len(new_plan_ids):
        print("MY LOG: Get all ids!")
    else:
        print("MY LOG: Not all ids was downloaded!")

    print("MY LOG: Extract service data...")
    url = "https://op.itmo.ru/api/academicplan/detail/"
    new_plans = []
    new_modules = []
    entry_date = datetime.now().isoformat()
    # Обрабатываем удаленные планы
    deleted_plan_ids = [plan_id for plan_id in plan_ids if plan_id not in new_plan_ids]
    if len(deleted_plan_ids):
        for deleted_plan_id in deleted_plan_ids:
            deleted_plan = None
            checked_date = "2000-11-16T14:50:15.097656"
            for plan in plans['plans']:
                if plan['id'] == deleted_plan_id and plan['entry_date'] > checked_date:
                    deleted_plan = plan
                    checked_date = plan['entry_date']

            if deleted_plan['is_deleted']:
                continue
                
            deleted_plan['entry_date'] = entry_date
            deleted_plan['is_deleted'] = True
            new_plans += [deleted_plan]
        
    # Обрабатываем измененные и добавленные планы, модули
    new_module_ids = []
    for new_plan_id in new_plan_ids:
        new_plan = requests.get(url + str(new_plan_id), headers=headers).json()
        # Если план не изменился, то не добавляем его
        if new_plan_id in plan_ids:
            checked_plan = None
            checked_date = "2000-11-16T14:50:15.097656"
            for plan in plans['plans']:
                if plan['id'] == new_plan_id and plan['entry_date'] > checked_date:
                    checked_plan = omit(plan, 'entry_date')
                    checked_plan = omit(checked_plan, 'is_deleted')
                    checked_date = plan['entry_date']
    
            if new_plan == checked_plan:
                continue
        
        for discipline_block in new_plan['discipline_blocks_in_academic_plan']:
            db_module_ids = []
            for new_module in discipline_block['modules_in_discipline_block']:
                new_module_id = int(new_module['id'])
                db_module_ids += [new_module_id]
                # Если модуль не изменился, то не добавляем его
                if new_module_id in module_ids:
                    checked_module = None
                    checked_date = "2000-11-16T14:50:15.097656"
                    for module in modules['modules']:
                        if module['id'] == new_module_id and module['entry_date'] > checked_date:
                            checked_module = omit(module, 'entry_date')
                            checked_module = omit(checked_module, 'is_deleted')
                            checked_date = module['entry_date']
    
                    if new_module == checked_module:
                        continue
    
                new_module['entry_date'] = entry_date
                new_module['is_deleted'] = False
                new_modules += [new_module]
    
            discipline_block['modules_in_discipline_block'] = db_module_ids
            new_module_ids += db_module_ids
    
        new_plan['entry_date'] = entry_date
        new_plan['is_deleted'] = False
        new_plans += [new_plan]

    # Обрабатываем удаленные модули
    new_module_ids = np.unique(new_module_ids)
    deleted_module_ids = [module_id for module_id in module_ids if module_id not in new_module_ids]
    if len(deleted_module_ids):
        for deleted_module_id in deleted_module_ids:
            deleted_module = None
            checked_date = "2000-11-16T14:50:15.097656"
            for module in modules['modules']:
                if module['id'] == deleted_module_id and module['entry_date'] > checked_date:
                    deleted_module = module
                    checked_date = module['entry_date']

            if deleted_module['is_deleted']:
                continue
                
            deleted_module['entry_date'] = entry_date
            deleted_module['is_deleted'] = True
            new_modules += [deleted_module]
    print("MY LOG: Complite!")

    print("MY LOG: Load staging data...")
    update_plans = dict(plans = plans['plans'] + new_plans)
    update_modules = dict(modules = modules['modules'] + new_modules)

    with open("./data/staging/staging_plans.json", 'w') as file:
        json.dump(update_plans, file)

    with open("./data/staging/staging_modules.json", 'w') as file:
        json.dump(update_modules, file)
    print("MY LOG: Complite!")
    # print(f"MY LOG: etl time - {time.strftime("%H:%M:%S", time.gmtime(time.time() - t))}")


with DAG(
    dag_id='staging_data',
    default_args=default_args,
    description="Perform ETL for the staging layer",
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    task = PythonOperator(task_id='etl_staging_data',
                           python_callable=etl_data)
    task
