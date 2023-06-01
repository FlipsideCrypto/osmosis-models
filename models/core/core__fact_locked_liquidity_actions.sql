{{ config(
    materialized = 'view'
) }}

SELECT
    A.block_id,
    A.block_timestamp,
    A.tx_id,
    A.tx_succeeded,
    A.msg_group,
    A.msg_type,
    A.msg_action,
    A.msg_action_description,
    A.locker_address,
    A.lock_id,
    A.amount,
    A.currency,
    A.decimal,
    A.pool_id,
    A.lock_duration,
    A.unlock_time,
    A.is_superfluid,
    A.unpool_new_lock_ids
FROM
    {{ ref('silver__locked_liquidity_actions') }} A
UNION ALL
SELECT
    A.block_id,
    A.block_timestamp,
    A.tx_id,
    A.tx_succeeded,
    A.msg_group,
    A.msg_type,
    A.msg_action,
    A.msg_action_description,
    A.locker_address,
    A.lock_id,
    amount,
    currency,
    DECIMAL,
    NULL AS pool_id,
    NULL AS lock_duration,
    NULL AS unlock_time,
    A.is_superfluid,
    NULL AS unpool_new_lock_ids
FROM
    {{ ref('silver__locked_liquidity_actions_convert') }} A
