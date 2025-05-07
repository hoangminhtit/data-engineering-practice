from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

def run_script(script_name):
    subprocess.run(['python', f'./scripts/{script_name}'])

default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False
}

with DAG('quotes_pipeline', schedule_interval='@daily', default_args=default_args, tags=['quotes'], description='Tự động cào và trực quan dữ liệu') as dag:

    crawl_task = PythonOperator(
        task_id='crawl_quotes',
        python_callable=lambda: run_script('crawl_quotes.py')
    )

    process_task = PythonOperator(
        task_id='process_data',
        python_callable=lambda: run_script('process_data.py')
    )

    visualize_task = PythonOperator(
        task_id='visualize',
        python_callable=lambda: run_script('visualize.py')
    )

    crawl_task >> process_task >> visualize_task
