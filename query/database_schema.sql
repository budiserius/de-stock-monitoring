CREATE SCHEMA IF NOT EXISTS silver;

-- Main Stocks Table
CREATE TABLE IF NOT EXISTS silver.stocks (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,    -- Changed from symbol to ticker
    datetime TIMESTAMPTZ NOT NULL, -- Changed from date to datetime; used TIMESTAMPTZ for the +00:00 offset
    open NUMERIC(15),
    high NUMERIC(15),
    low NUMERIC(15),
    close NUMERIC(15),
    volume BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Idempotency Index
-- Ensures no duplicate records for the same ticker at the same minute
CREATE UNIQUE INDEX IF NOT EXISTS idx_silver_stocks_ticker_datetime 
ON silver.stocks(ticker, datetime);

-- Audit Log Table
CREATE TABLE IF NOT EXISTS silver.ingestion_logs (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(100),
    run_id VARCHAR(100),
    event_name VARCHAR(50), 
    status VARCHAR(20),     
    rows_affected INT DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);