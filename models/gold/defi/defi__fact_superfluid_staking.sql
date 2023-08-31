{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STAKING' }} }
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    action,
    delegator_address,
    amount,
    currency,
    DECIMAL,
    validator_address,
    lock_id,
    IFF(
        action = 'undelegate'
        AND currency IS NOT NULL,
        TRUE,
        FALSE
    ) AS is_unpool
FROM
    {{ ref('silver__superfluid_staking') }}
UNION ALL
SELECT
    A.block_id,
    A.block_timestamp,
    A.tx_id,
    A.tx_succeeded,
    A.msg_action AS action,
    A.locker_address AS delegator_address,
    amount,
    currency,
    DECIMAL,
    A.validator_address,
    A.lock_id,
    FALSE AS is_unpool
FROM
    {{ ref('silver__locked_liquidity_actions_convert') }} A
