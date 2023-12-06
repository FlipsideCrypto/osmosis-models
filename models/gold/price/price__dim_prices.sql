{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'PRICES' }} },
    tags = ['noncore']
) }}

SELECT
    recorded_at,
    symbol,
    currency,
    price,
    total_supply,
    volume_24h,
    provider,
    prices_combined_id AS dim_prices_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__prices_combined') }}
