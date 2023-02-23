{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH

{% if is_incremental() %}
max_date AS (

    SELECT
        MAX(
            _inserted_timestamp
        ) _inserted_timestamp
    FROM
        {{ this }}
),
{% endif %}

base_txn AS (
    SELECT
        A.block_id,
        A.block_timestamp,
        A.tx_id,
        A.tx_succeeded,
        A.msg_group,
        A.msg_type,
        A.msg_action_description,
        A.locker_address,
        A.lock_id,
        A.amount,
        A.currency,
        A.decimal,
        A.pool_id,
        A.lock_duration,
        A.unlock_time,
        A.unpool_new_lock_ids,
        A._unique_key,
        A._inserted_timestamp
    FROM
        {{ ref('silver__locked_liquidity_actions') }} A
    WHERE
        is_superfluid = TRUE

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        _inserted_timestamp
    FROM
        max_date
)
{% endif %}
),
vals AS (
    SELECT
        lock_id,
        validator_address
    FROM
        {{ ref('silver__superfluid_actions') }} A
    WHERE
        validator_address IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        _inserted_timestamp
    FROM
        max_date
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY lock_id
ORDER BY
    block_id DESC) = 1)
),
unpool_lock_val AS (
    SELECT
        A.lock_id,
        A.currency,
        A.delegator_address,
        A.validator_address
    FROM
        {{ ref('silver__superfluid_actions') }} A
        JOIN {{ ref('silver__locked_liquidity_actions') }}
        b
        ON b.msg_action_description = 'unpool'
        AND A.delegator_address = b.locker_address
        AND A.currency = b.currency
        AND A.block_id < b.block_id
    WHERE
        validator_address IS NOT NULL

{% if is_incremental() %}
AND A._inserted_timestamp >= (
    SELECT
        _inserted_timestamp
    FROM
        max_date
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY A.lock_id
ORDER BY
    A.block_id DESC) = 1)
)
SELECT
    A.block_id,
    A.block_timestamp,
    A.tx_id,
    A.msg_group,
    A.tx_succeeded,
    CASE
        WHEN msg_action_description = 'initial lock' THEN 'delegate'
        WHEN msg_action_description = 'add to position' THEN 'delegate'
        WHEN msg_action_description = 'unlock' THEN 'undelegate'
        WHEN msg_action_description = 'unpool' THEN 'undelegate'
        WHEN msg_action_description LIKE '%ulock%'
        OR msg_action_description LIKE '%undelegate%' THEN 'undelegate'
    END AS action,
    A.locker_address AS delegator_address,
    A.amount,
    A.currency,
    A.decimal,
    COALESCE(
        b.validator_address,
        C.validator_address
    ) AS validator_address,
    COALESCE(
        A.lock_id,
        C.lock_id
    ) AS lock_id,
    _inserted_timestamp,
    concat_ws(
        '-',
        A.tx_id,
        A.msg_group,
        COALESCE(
            A.lock_id,
            -1
        ),
        msg_action_description
    ) AS _unique_key
FROM
    base_txn A
    LEFT JOIN vals b
    ON A.lock_id = b.lock_id
    LEFT JOIN unpool_lock_val C
    ON A.locker_address = C.delegator_address
    AND A.currency = C.currency
    AND A.msg_action_description = 'unpool'
