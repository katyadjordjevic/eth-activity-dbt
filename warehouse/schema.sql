-- Create layer schemas (Bronze / Silver / Gold)
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- BRONZE: Raw Etherscan transactions (JSON payload)
CREATE TABLE IF NOT EXISTS bronze.etherscan_transactions_raw (
    tx_hash TEXT PRIMARY KEY,
    raw_payload JSONB NOT NULL,
    ingested_at TIMESTAMP DEFAULT NOW()
);

-- BRONZE: Raw CoinGecko ETH daily price (JSON payload)
CREATE TABLE IF NOT EXISTS bronze.coingecko_eth_price_raw (
    date DATE PRIMARY KEY,
    raw_payload JSONB NOT NULL,
    ingested_at TIMESTAMP DEFAULT NOW()
);