{{ config(
    materialized = 'view',
    tags = ['noncore']
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    liquidity_provider_address,
    action,
    pool_id :: ARRAY AS pool_id,
    amount,
    currency,
    DECIMAL,
    msg_group,
    COALESCE(
        liquidity_provider_actions_id,
        {{ dbt_utils.generate_surrogate_key(
            ['_unique_key']
        ) }}
    ) AS fact_liquidity_provider_actions_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__liquidity_provider_actions') }}
UNION ALL
SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    liquidity_provider_address,
    action,
    pool_id,
    amount,
    currency,
    DECIMAL,
    msg_group,
    COALESCE(
        early_liquidity_provider_actions_id,
        {{ dbt_utils.generate_surrogate_key(
            ['_unique_key']
        ) }}
    ) AS fact_liquidity_provider_actions_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__early_liquidity_provider_actions') }}
UNION ALL
SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    liquidity_provider_address,
    action,
    pool_id :: ARRAY AS pool_id,
    amount,
    currency,
    DECIMAL,
    msg_group,
    COALESCE(
        liquidity_provider_actions_unpool_id,
        {{ dbt_utils.generate_surrogate_key(
            ['_unique_key']
        ) }}
    ) AS fact_liquidity_provider_actions_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__liquidity_provider_actions_unpool') }}
