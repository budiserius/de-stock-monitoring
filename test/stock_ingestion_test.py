import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dags.scripts.bronze_utils import fetch_stock_data
result = fetch_stock_data(['MTDL.JK', 'BBCA.JK', 'GOTO.JK'], './test/stock_ingestion_result.parquet')

if result:
    print("Test Success")
else:
    print('Test Failed')