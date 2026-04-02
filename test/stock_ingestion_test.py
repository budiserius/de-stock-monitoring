import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dags.scripts.stock_utils import fetch_stock_data
result = fetch_stock_data(['MTDL.JK'], './scripts/stock_ingestion_result.parquet')

if result:
    print("Test Success")
else:
    print('Test Failed')