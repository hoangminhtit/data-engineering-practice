from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import pandas as pd
import os
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

default_args = {
    'owner': 'airflow',
    'retries': 1,
}
dag = DAG(
    dag_id='DATA_ENGINEER_EXERCISE',
    description='A simple DAG',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False
)

Bai1 = BashOperator(
    task_id='collect_extract_zip_files',
    bash_command='python /var/tmp/app/Exercise-1/main.py',
    dag=dag)

Bai2 = BashOperator(
    task_id='crawl_weather',
    bash_command ='python /var/tmp/app/Exercise-2/main.py',
    dag=dag
)

Bai3 = BashOperator(
    task_id='web_scrapping',
    bash_command ='python /var/tmp/app/Exercise-3/main.py',
    dag=dag
)

Bai4 = BashOperator(
    task_id='convert_json_to_csv',
    bash_command = 'python /var/tmp/app/Exercise-4/main.py',
    dag=dag
)

Bai5 = BashOperator(
    task_id='import_data',
    bash_command ='python /var/tmp/app/Exercise-5/main.py',
    dag=dag
)

Bai1
Bai2
Bai3
Bai4
Bai5