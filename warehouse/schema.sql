-- Raw Ethereum transactions from Etherscan
CREATE TABLE IF NOT EXISTS raw_transactions (
	tx_hash TEXT PRIMARY KEY,
	block_number BIGINT,
	block_timestamp TIMESTAMP,
	from_address TEXT,
	to_address TEXT,
	value_eth NUMERIC,
	gas_used BIGINT,
	gas_price_wei BIGINT,
	raw_payload JSONB,
	ingested_at TIMESTAMP DEFAULT NOW()
);

-- Raw daily ETH price from CoinGecko
CREATE TABLE IF NOT EXISTS raw_eth_price_daily (
	date DATE PRIMARY KEY,
	price_usd NUMERIC,
	raw_payload JSONB,
	ingested_at TIMESTAMP DEFAULT NOW()
);