import yfinance as yf
import pandas as pd
import os
from datetime import datetime

def fetch_stock_data(tickers, output_path):
    print(f"Mengambil data untuk: {tickers}")
    data = yf.download(tickers, period="1d", interval="1m")
    
    if data.empty:
        return None

    df = data['Close'].reset_index()
    df = pd.melt(df, id_vars=['Datetime'], var_name='ticker', value_name='price')
    df['ingestion_time'] = datetime.now()

    # Pastikan folder ada
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    df.to_parquet(output_path, index=False)
    return output_path