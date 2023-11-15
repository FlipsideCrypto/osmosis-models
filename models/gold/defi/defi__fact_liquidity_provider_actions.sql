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
    msg_group
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
    msg_group
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
    msg_group
FROM
    {{ ref('silver__liquidity_provider_actions_unpool') }}
