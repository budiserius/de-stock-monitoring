# stocks_medallion_dag.py
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
        print("No path received from Bronze task.")
        return
        
    if isinstance(path, (list, tuple)):
        path = path[0]

    pg_hook = PostgresHook(postgres_conn_id='postgre_local_stock_monitoring')
    engine = pg_hook.get_sqlalchemy_engine()
    
    silver_utils.transform_and_upsert(
        parquet_path=path,
        engine=engine,
        dag_id=context['dag'].dag_id,
        run_id=context['run_id']
    )

with DAG(
    dag_id='stock_medallion_daily',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily', 
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'execution_timeout': timedelta(minutes=10) # Daily task punya ruang lebih lama
    },
    template_searchpath='/opt/airflow/dags/scripts',
    tags=['monitoring', 'daily', 'stocks']
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