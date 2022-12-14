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
    action,
    delegator_address,
    amount,
    currency,
    DECIMAL,
    validator_address,
    lock_id
FROM
    {{ ref('silver__superfluid_staking') }}
