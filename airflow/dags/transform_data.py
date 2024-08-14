from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def run_transformations():
    import subprocess
    subprocess.run(["python", "/app/test_transformer.py"])
#     subprocess.run(["python", "/app/transformer.py"])


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 27),
    'retries': 1,
}

dag = DAG('transform_data', default_args=default_args, schedule_interval='@hourly')

run_transform = PythonOperator(
    task_id='run_transform',
    python_callable=run_transformations,
    dag=dag,
)

run_transform

