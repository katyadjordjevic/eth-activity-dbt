import os
import json
import random
import datetime as dt   
import time
import urllib.request
import urllib.error
from typing import Any
import psycopg2

URL =  "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd"

def fetch_with_backoff(url: str, max_attempts: int = 6, base_delay: float =1.0) -> dict[str,Any]:
    """
    Retries transient failures with exponential backoff + jitter.
    Retries on: 429, 5xx, timeouts, temporary network errors.
    """
    for attempt in range(1, max_attempts + 1):
        try:    
            req = urllib.request.Request(
                url, 
                headers = {
                    "Accept": "application/json",
                    "User-Agent": "eth-activity-dbt",
                },
            )
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode("utf-8"))
        
        except urllib.error.HTTPError as e:
            status = getattr(e, "code", None)
            retryable = status in (429, 500, 502, 503, 504)
            
            if not retryable or attempt == max_attempts:
                raise
            
            retry_after = e.headers.get("Retry-After")
            if retry_after:
                sleep_s = float (retry_after)
            else:
                sleep_s = base_delay * (2 **(attempt - 1))+ random.random()
                
            time.sleep(sleep_s)
        
        except (urllib.error.URLError, TimeoutError):
            if attempt == max_attempts:
                raise
            sleep_s = base_delay * (2 ** (attempt - 1)) + random.random()
            time.sleep(sleep_s)
    raise RuntimeError ("fetch_with_backoff exausted retries unexpectedly")

def main():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise SystemExit ("DATABASE_URL is not set. Load it from .env before running.")
    
    today = dt.date.today()
    
    raw: dict[str, Any] = fetch_with_backoff(URL)
    
    payload: dict[str, Any] = {
        "source": "coingecko",
        "endpoint": "simple/price",
        "data": raw,
        "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    
    sql = """
    INSERT INTO bronze.coingecko_eth_price_raw (date, raw_payload)
    VALUES (%s, %s::jsonb)
    ON CONFLICT (date)
    DO UPDATE SET
        raw_payload = EXCLUDED.raw_payload,
        ingested_at = NOW();
    """
    with psycopg2.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (today, json.dumps(payload)))
        conn.commit()
        
    print(f"✅ Upserted ETH price for {today}")

if __name__ == "__main__":
    main()