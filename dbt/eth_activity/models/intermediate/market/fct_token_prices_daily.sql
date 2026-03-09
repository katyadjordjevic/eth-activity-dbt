{{ config(
    materialized='incremental',
    unique_key=['price_date', 'token']
) }}

SELECT
    date as price_date,
    eth_usd_price as token_price,
    fetched_at,
    source,
    'ETH' as token
FROM {{ ref('stg_coingecko_eth_price') }}


{% if is_incremental() %}
where price_date >= (
    select coalesce(max(price_date), '1900-01-01')
    from {{ this }}
)
{% endif %}