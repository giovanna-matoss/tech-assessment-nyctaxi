from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'streaming_taxi_fares',
    default_args=default_args,
    description='DAG para ingestão contínua de dados no Kafka',
    schedule_interval='@weekly',  # Processa os dados uma vez por semana
    start_date=datetime(2025, 3, 6),
    catchup=False,
)

# Etapa para iniciar o processo de ingestão
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

analyze_revenue = BashOperator(
    task_id='analyze_revenue',
    bash_command='python3 /opt/airflow/scripts/pipelines/analyzeRevenue.py --year {{ dag_run.conf["year"] }} --month {{ dag_run.conf["month"] }} --day {{ dag_run.conf["day"] }}',
    dag=dag,
)

analyze_revenue
