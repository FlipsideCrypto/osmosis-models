{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEFI, PRICES' }} }
) }}

SELECT
    block_id,
    block_date,
    currency,
    market_cap,
    price,
    price_usd,
    liquidity,
    liquidity_usd,
    volume,
    volume_usd
FROM
    {{ ref('silver__token_summary_day') }}
