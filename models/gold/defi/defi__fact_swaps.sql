{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'SWAPS' }} },
    tags = ['noncore']
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    trader,
    from_amount,
    from_currency,
    from_decimal,
    to_amount,
    to_currency,
    TO_DECIMAL,
    pool_ids,
    _BODY_INDEX
FROM
    {{ ref('silver__swaps') }}
