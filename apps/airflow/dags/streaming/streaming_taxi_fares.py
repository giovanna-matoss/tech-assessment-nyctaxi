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
    schedule_interval=None,
    start_date=datetime(2025, 3, 6),
    catchup=False,
)

# Etapa para iniciar o processo de ingestão
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

# Etapa de execução do produtor
producer_task = BashOperator(
    task_id='run_producer',
    bash_command='python3 /opt/airflow/scripts/producers/generate_events.py',
    dag=dag,
)

# Etapa de execução do consumidor com datas
consumer_task = BashOperator(
    task_id='run_consumer',
    bash_command='python3 /opt/airflow/scripts/consumers/consume_events.py --start_date {{ dag_run.conf["start_date"] }} --end_date {{ dag_run.conf["end_date"] }}',
    dag=dag
)

# Fluxo de execução das tasks
start_task >> producer_task >> consumer_task
