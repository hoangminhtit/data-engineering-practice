from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
import webbrowser

def visualize():
    print('https://charts.mongodb.com/charts-project-0-octxxbu/public/dashboards/ad6298d8-8fd3-476d-a4c6-c86ee18cc535')

default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False
}

with DAG('trading_economics_pipeline', 
        schedule_interval='@daily', 
        default_args=default_args, 
        description='Tự động cào và trực quan dữ liệu') as dag:

    crawl_task = BashOperator(
        task_id='crawl_quotes',
        bash_command = 'python /var/tmp/app/crawl_trading_economics.py'
    )

    visualize_task = PythonOperator(
        task_id='visualize',
        python_callable=visualize
    )

crawl_task >> visualize_task
