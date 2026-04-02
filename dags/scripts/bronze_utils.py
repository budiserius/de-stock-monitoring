# bronze_utils.py
import yfinance as yf
import pandas as pd
import os
from datetime import datetime

def fetch_stock_data(tickers, base_folder):
    try:
        data = yf.download(tickers, period="1d", interval="1m", group_by='ticker', auto_adjust=True)
        
        if data.empty:
            print(f"Warning: No data found for tickers {tickers}")
            return None

        if isinstance(data.columns, pd.MultiIndex):
            df = data.stack(level=0, future_stack=True).reset_index()
            df.rename(columns={'level_1': 'ticker'}, inplace=True)
        else:
            df = data.reset_index()
            df['ticker'] = tickers[0]

        df.columns = [col.lower().replace(' ', '_') for col in df.columns]

        os.makedirs(base_folder, exist_ok=True)
        today = datetime.now().strftime("%Y-%m-%d")
        base_path = os.path.join(base_folder, f"stocks_{today}")
        parquet_path = f"{base_path}.parquet"
        csv_path = f"{base_path}.csv"

        df.to_parquet(parquet_path, index=False, engine='pyarrow')
        # df.to_csv(csv_path, index=False)

        print(f"Success: Files saved to {base_folder} | Rows: {len(df)}")
        return parquet_path

    except Exception as e:
        print(f"Error fetching data: {str(e)}")
        raise e
    