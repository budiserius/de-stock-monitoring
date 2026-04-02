import yfinance as yf
import pandas as pd
import os
from datetime import datetime

def fetch_stock_data(tickers, base_folder):
    # 1. Download data
    data = yf.download(tickers, period="1d", interval="1m", group_by='ticker')
    
    if data.empty:
        print("Data kosong.")
        return None

    # 2. Transformasi ke Long Format
    df = data.stack(level=0, future_stack=True).reset_index()

    # 3. Mapping Kolom secara Pintar
    date_cols = df.select_dtypes(include=['datetime64', 'datetimetz']).columns.tolist()
    
    if not date_cols:
        potential_date_cols = [c for c in df.columns if c.lower() in ['datetime', 'index', 'date']]
        date_col = potential_date_cols[0]
    else:
        date_col = date_cols[0]

    ohlcv_cols = ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
    other_cols = [c for c in df.columns if c not in ohlcv_cols and c != date_col]
    symbol_col = other_cols[0]

    # 4. Filter dan Standarisasi Nama
    df_clean = df[[date_col, symbol_col, 'Close']].copy()
    df_clean.columns = ['date', 'symbol', 'close']

    # 5. Ambil data menit terakhir saja
    latest_ts = df_clean['date'].max()
    df_latest = df_clean[df_clean['date'] == latest_ts].copy()

    # 6. Simpan ke CSV
    os.makedirs(base_folder, exist_ok=True)
    file_ts = latest_ts.strftime('%Y%m%d_%H%M')
    file_name = f"stocks_{file_ts}.csv"
    file_path = os.path.join(base_folder, file_name)
    
    df_latest.to_csv(file_path, index=False)
    
    print(f"Bronze: Berhasil menyimpan data menit {file_ts} untuk {tickers}")
    return file_path

if __name__ == "__main__":
    # Ticker untuk testing
    tickers = ["MTDL.JK"]
    
    # Folder penyimpanan (silakan ubah sesuai kebutuhan)
    base_folder = "./data_stocks"
    
    # Panggil fungsi
    file_path = fetch_stock_data(tickers, base_folder)
    
    # Tampilkan hasil file
    if file_path:
        print(f"File berhasil dibuat di: {file_path}")