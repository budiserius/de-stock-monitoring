# silver_utils.py
import pandas as pd
from datetime import datetime
from sqlalchemy import text

def transform_and_upsert(parquet_path, engine, dag_id, run_id):
    df = pd.read_parquet(parquet_path)
    df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
    df = df.sort_values(['ticker', 'datetime'])
    df[['open', 'high', 'low', 'close', 'volume']] = df.groupby('ticker')[['open', 'high', 'low', 'close', 'volume']].ffill()
    
    required_columns = ['ticker', 'datetime', 'open', 'high', 'low', 'close', 'volume']
    df = df[required_columns].copy()
    
    df['updated_at'] = datetime.now()
    

    df.to_sql('temp_stocks', engine, if_exists='replace', index=False)
    
    upsert_sql = text("""
        INSERT INTO silver.stocks (ticker, datetime, open, high, low, close, volume, updated_at)
        SELECT ticker, datetime, open, high, low, close, volume, updated_at 
        FROM temp_stocks
        ON CONFLICT (ticker, datetime) 
        DO UPDATE SET 
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            updated_at = EXCLUDED.updated_at;
    """)
    
    log_sql = text("""
        INSERT INTO silver.ingestion_logs (dag_id, run_id, event_name, status, rows_affected)
        VALUES (:dag_id, :run_id, 'BRONZE_TO_SILVER', 'SUCCESS', :rows_affected);
    """)

    error_log_sql = text("""
        INSERT INTO silver.ingestion_logs (dag_id, run_id, event_name, status, error_message)
        VALUES (:dag_id, :run_id, 'BRONZE_TO_SILVER', 'FAILED', :error_msg);
    """)

    try:
        with engine.begin() as conn:
            conn.execute(upsert_sql)
            conn.execute(log_sql, {
                "dag_id": dag_id, 
                "run_id": run_id, 
                "rows_affected": len(df)
            })
        print(f"Successfully processed {len(df)} rows.")
        
    except Exception as e:
        with engine.begin() as conn:
            conn.execute(error_log_sql, {
                "dag_id": dag_id, 
                "run_id": run_id, 
                "error_msg": str(e)
            })
        raise e