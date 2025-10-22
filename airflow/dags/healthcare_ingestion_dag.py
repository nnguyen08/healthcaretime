from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.insert(0, project_root)

from src.s3_helper import S3Helper
from src.appointment_generator import AppointmentGenerator
NUM_APPOINTMENTS_PER_RUN = 200

def generate_appointments(**context):
    execution_date = context['execution_date']
    year = execution_date.year
    month = execution_date.month
    day = execution_date.day
    
    output_dir = f"data/partitioned/year={year}/month={month:02d}/day={day:02d}"
    filename = f"appointments_{year}-{month:02d}-{day:02d}.csv"
    start_date = execution_date.strftime('%Y-%m-%d')
    end_date = execution_date.strftime('%Y-%m-%d')
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, filename)
    generator = AppointmentGenerator()
    df = generator.generate_appointments(NUM_APPOINTMENTS_PER_RUN, start_date, end_date)
    df.to_csv(filepath, index=False)
    return filepath

with DAG(
    dag_id='healthcare_ingestion',
    start_date=datetime(2024,1,1),
    schedule='@hourly',
    catchup=False
) as dag:
    generate_task = PythonOperator(
        task_id = 'generate_appointments',
        python_callable=generate_appointments
    )
    