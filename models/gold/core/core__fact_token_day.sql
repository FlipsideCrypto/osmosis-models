{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEFI, PRICES' }} },
    tags = ['noncore']
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
    volume_usd,
    COALESCE(
        token_summary_day_id,
        {{ dbt_utils.generate_surrogate_key(
            ['currency', 'block_date']
        ) }}
    ) AS fact_token_day_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__token_summary_day') }}
