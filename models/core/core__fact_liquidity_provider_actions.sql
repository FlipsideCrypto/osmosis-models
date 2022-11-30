{{ config(
    materialized = 'view'
) }}

SELECT
    block_id,
    block_timestamp,
    blockchain,
    chain_id,
    tx_id,
    tx_status,
    tx_succeeded,
    liquidity_provider_address,
    action,
    pool_id,
    amount,
    currency,
    DECIMAL
FROM
    {{ ref('silver__liquidity_provider_actions') }}
UNION ALL
SELECT
    block_id,
    block_timestamp,
    blockchain,
    chain_id,
    tx_id,
    tx_status,
    tx_succeeded,
    liquidity_provider_address,
    action,
    pool_id,
    amount,
    currency,
    DECIMAL
FROM
    {{ ref('silver__early_liquidity_provider_actions') }}
