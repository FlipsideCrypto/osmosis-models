{{ config(
    materialized = 'view'
) }}

SELECT 
    recorded_at, 
    symbol, 
    price, 
    total_supply, 
    volume_24h, 
    provider
FROM {{ ref('silver__prices') }}

UNION ALL 

SELECT 
    block_hour AS recorded_at, 
    project_name AS symbol, 
    price_usd AS price, 
    NULL AS total_supply, 
    NULL AS volume_24h, 
    'swaps' AS provider 

FROM {{ ref('silver__prices_swaps') }}