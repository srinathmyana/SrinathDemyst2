import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Add the path to where process_anonymize_csv.py is located
sys.path.append('/mnt/c/Users/Srinath/Desktop/Demyst2')

# Import the function
from process_anonymize_csv import process_large_csv

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your_email@example.com'],  # Optional: Email notifications
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': datetime(2024, 12, 1),
    'retries': 1,
}

# Define the DAG
with DAG(
    'anonymize_large_csv',
    default_args=default_args,
    description='A DAG for anonymizing large CSV files',
    schedule_interval=None,  # Correct parameter
    catchup=False,  # Prevent backfill
) as dag:

    # Python task for anonymizing CSV
    anonymize_task = PythonOperator(
        task_id='anonymize_csv',
        python_callable=process_large_csv,
        op_kwargs={
            'input_csv': '/mnt/c/Users/Srinath/Desktop/Demyst2/large_input.csv',
            'output_csv': '/mnt/c/Users/Srinath/Desktop/Demyst2/output/anonymized_large_output.csv',
            'chunk_size': 1000,  # Adjust as needed
        },
    )

    anonymize_task
