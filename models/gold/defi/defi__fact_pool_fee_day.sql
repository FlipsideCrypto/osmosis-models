{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEFI' }} },
    tags = ['noncore']
) }}

SELECT
    block_id,
    block_date,
    pool_id,
    currency,
    fees,
    fees_usd,
    fee_type,
    COALESCE(
        pool_fee_summary_day_id,
        {{ dbt_utils.generate_surrogate_key(
            ['pool_id','block_date','currency']
        ) }}
    ) AS fact_pool_fee_day_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__pool_fee_summary_day') }}
