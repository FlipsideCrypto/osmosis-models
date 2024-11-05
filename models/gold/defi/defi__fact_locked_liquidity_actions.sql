{{ config(
    materialized = 'view',
    tags = ['noncore'],
    enabled = false
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
    A.unpool_new_lock_ids,
    COALESCE(
        locked_liquidity_actions_id,
        {{ dbt_utils.generate_surrogate_key(
            ['_unique_key']
        ) }}
    ) AS fact_locked_liquidity_actions_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
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
    NULL AS unpool_new_lock_ids,
    COALESCE(
        locked_liquidity_actions_convert_id,
        {{ dbt_utils.generate_surrogate_key(
            ['_unique_key']
        ) }}
    ) AS fact_locked_liquidity_actions_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__locked_liquidity_actions_convert') }} A
