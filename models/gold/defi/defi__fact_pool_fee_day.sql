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
    fee_type
FROM
    {{ ref('silver__pool_fee_summary_day') }}
