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
    pool_id,
    _body_index,
    COALESCE(
        swaps_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','_body_index']
        ) }}
    ) AS fact_swaps_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__swaps') }}
UNION ALL
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
    pool_id :: ARRAY AS pool_ids,
    pool_id,
    msg_index AS _body_index,
    COALESCE(
        swaps_transfers_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','_body_index']
        ) }}
    ) AS fact_swaps_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__swaps_transfers') }}
