{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEFI, PRICES' }} },
    tags = ['noncore']
) }}

SELECT
    block_id,
    block_timestamp,
    pool_id,
    token_0_denom AS token_0_currency,
    token_0_amount,
    token_1_denom AS token_1_currency,
    token_1_amount,
    price,
    price_usd,
    twap,
    twap_usd,
    liquidity,
    liquidity_usd,
    volume,
    volume_usd,
    token_2_denom AS token_2_currency,
    token_2_amount,
    token_3_denom AS token_3_currency,
    token_3_amount,
    COALESCE(
        pool_summary_hour_id,
        {{ dbt_utils.generate_surrogate_key(
            ['pool_id','block_hour']
        ) }}
    ) AS fact_pool_hour_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__pool_summary_hour') }}
