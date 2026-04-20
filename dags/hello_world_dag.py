from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import logging

def process_data():
    logging.info("Starting data processing task...")
    # Tạo một dataframe đơn giản bằng pandas
    data = {
        'Name': ['Alice', 'Bob', 'Charlie'],
        'Age': [25, 30, 35],
        'City': ['New York', 'London', 'Paris']
    }
    df = pd.DataFrame(data)
    logging.info(f"Dataframe created:\n{df}")
    
    # Tính tuổi trung bình
    avg_age = df['Age'].mean()
    logging.info(f"Average Age: {avg_age}")
    return f"Processed {len(df)} rows. Avg age: {avg_age}"

# Cấu hình DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'example_pandas_dag',
    default_args=default_args,
    description='Một DAG đơn giản sử dụng Pandas',
    schedule=timedelta(days=1),
    catchup=False,
    tags=['example', 'pandas'],
) as dag:

    task_hello = PythonOperator(
        task_id='hello_task',
        python_callable=lambda: logging.info("Hello from Airflow!"),
    )

    task_process = PythonOperator(
        task_id='process_pandas_data',
        python_callable=process_data,
    )

    task_hello >> task_process
