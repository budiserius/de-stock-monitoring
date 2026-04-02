from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from dags.scripts.stock_utils import fetch_stock_data  # Import fungsi

with DAG(
    dag_id='stock_ingestion',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@hourly',
    catchup=False
) as dag:

    task_extract = PythonOperator(
    task_id='extract_stocks',
    python_callable=fetch_stock_data,
    op_kwargs={
        'tickers': ['MTDL.JK', 'BBCA.JK'],
        'output_path': '/opt/airflow/dags/data_output/stocks_{{ ts_nodash }}.parquet'
    }
)