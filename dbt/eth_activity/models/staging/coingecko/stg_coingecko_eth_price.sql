{{ config(materialized='view') }}

SELECT
    date,
    (raw_payload -> 'data' -> 'ethereum' ->> 'usd')::numeric AS eth_usd_price,
    (raw_payload ->> 'fetched_at')::timestamptz AS fetched_at,
    ingested_at,
    raw_payload ->> 'source' AS source
FROM {{ source('bronze', 'coingecko_eth_price_raw') }}
WHERE raw_payload -> 'data' -> 'ethereum' ->> 'usd' IS NOT NULL
