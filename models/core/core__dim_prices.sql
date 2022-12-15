{{ config(
    materialized = 'view'
) }}

SELECT
    recorded_hour AS recorded_at,
    symbol,
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
    price_usd AS price,
    NULL AS total_supply,
    NULL AS volume_24h,
    'swaps' AS provider
FROM
    {{ ref('silver__prices_swaps') }}
