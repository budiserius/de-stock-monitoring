from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "scripts"))
import bronze_utils
import silver_utils

def run_silver_step(**context):
    path = context['ti'].xcom_pull(task_ids='bronze_ingestion_task')
    
    if not path:
        print("No new data to process.")
        return
        
    pg_hook = PostgresHook(postgres_conn_id='postgre_local_stock_monitoring')
    engine = pg_hook.get_sqlalchemy_engine()
    
    silver_utils.transform_and_upsert(
        parquet_path=path,
        engine=engine,
        dag_id=context['dag'].dag_id,
        run_id=context['run_id']
    )

with DAG(
    dag_id='stock_medallion_realtime',
    start_date=datetime(2024, 1, 1),
    # RUN SETIAP MENIT (Cron format: menit, jam, hari, bulan, hari_minggu)
    schedule_interval='* * * * *', 
    catchup=False,
    max_active_runs=1, # Mencegah task tumpang tindih
    default_args={
        'retries': 0, # Jangan retry berlebihan untuk menit-an
        'execution_timeout': timedelta(seconds=55) # Harus selesai < 1 menit
    },
    tags=['monitoring', 'realtime']
) as dag:

    bronze_task = PythonOperator(
        task_id='bronze_ingestion_task',
        python_callable=bronze_utils.fetch_stock_data,
        op_kwargs={
            'tickers': ['MTDL.JK', 'BBCA.JK', 'GOTO.JK'],
            'base_folder': '/opt/airflow/dags/data_output'
        }
    )

    silver_task = PythonOperator(
        task_id='silver_processing_task',
        python_callable=run_silver_step
    )

    bronze_task >> silver_task