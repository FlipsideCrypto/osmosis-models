{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'PRICES' }} }
) }}

SELECT
    recorded_hour AS recorded_at,
    symbol,
    NULL AS currency,
    CLOSE AS price,
    NULL AS total_supply,
    NULL AS volume_24h,
    'coin gecko' AS provider
FROM
    {{ ref('silver__token_prices_coin_gecko_hourly') }}
UNION ALL
SELECT
    recorded_hour AS recorded_at,
    symbol,
    NULL AS currency,
    CLOSE AS price,
    NULL AS total_supply,
    NULL AS volume_24h,
    'coin market cap' AS provider
FROM
    {{ ref('silver__token_prices_coin_market_cap_hourly') }}
UNION ALL
SELECT
    block_hour AS recorded_at,
    project_name AS symbol,
    currency,
    price_usd AS price,
    NULL AS total_supply,
    NULL AS volume_24h,
    'swaps' AS provider
FROM
    {{ ref('silver__prices_swaps') }}
UNION ALL
SELECT
    DATE_TRUNC(
        'hour',
        block_timestamp
    ) AS recorded_at,
    project_name AS symbol,
    token_address AS currency,
    price_usd AS price,
    NULL AS total_supply,
    NULL AS volume_24h,
    'pool balance' AS provider
FROM
    {{ ref('silver__pool_token_prices_usd') }} A
    LEFT JOIN {{ ref('silver__asset_metadata') }}
    b
    ON A.token_address = b.address
