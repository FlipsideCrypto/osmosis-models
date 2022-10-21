{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH all_staked AS (

    SELECT
        block_id,
        block_timestamp,
        locker_address AS address,
        SUM(amount) amount,
        currency,
        DECIMAL,
        lock_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__locked_liquidity_actions') }}
        s
    WHERE
        msg_action_description IN (
            'initial lock',
            'add to position'
        )
        AND amount > 0

{% if is_incremental() %}
AND block_timestamp :: DATE >=(
    SELECT
        DATEADD('day', -2, MAX(block_timestamp))
    FROM
        {{ this }})
    {% endif %}
    GROUP BY
        block_id,
        block_timestamp,
        locker_address,
        currency,
        DECIMAL,
        lock_id,
        _inserted_timestamp
),
undel_base AS (
    SELECT
        DISTINCT block_id,
        block_timestamp,
        locker_address AS address,
        currency,
        DECIMAL,
        lock_id,
        msg_action_description,
        tx_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__locked_liquidity_actions') }}
    WHERE
        msg_action_description IN (
            'unlock',
            'unlock-undelegate',
            'unpool'
        )

{% if is_incremental() %}
AND block_timestamp :: DATE >=(
    SELECT
        DATEADD('day', -2, MAX(block_timestamp))
    FROM
        {{ this }})
    {% endif %}
),
unpool_lock_val AS (
    SELECT
        A.lock_id,
        A.currency,
        A.locker_address,
        b.block_id,
        b.tx_id
    FROM
        {{ ref('silver__locked_liquidity_actions') }} A
        JOIN undel_base b
        ON b.msg_action_description = 'unpool'
        AND A.locker_address = b.address
        AND A.currency = b.currency
        AND A.block_id < b.block_id
    WHERE
        A.lock_id IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY A.lock_id
    ORDER BY
        A.block_id DESC) = 1)
),
undel_bal AS (
    SELECT
        A.lock_id,
        A.currency,
        A.decimal,
        SUM(COALESCE(amount, 0)) amount
    FROM
        {{ ref('silver__locked_liquidity_actions') }} A
        LEFT JOIN unpool_lock_val C
        ON A.locker_address = C.locker_address
        AND A.currency = C.currency
        AND A.block_id <= C.block_id
        LEFT JOIN undel_base b
        ON A.lock_id = b.lock_id
        LEFT JOIN undel_base bb
        ON C.tx_id = bb.tx_id
    WHERE
        (
            bb.address IS NOT NULL
            OR b.address IS NOT NULL
        )
        AND A.amount > 0
        AND A.msg_action_description IN (
            'initial lock',
            'add to position'
        )
    GROUP BY
        A.lock_id,
        A.currency,
        A.decimal
),
combine AS (
    SELECT
        block_id,
        block_timestamp,
        address,
        lock_id,
        amount,
        currency,
        DECIMAL,
        _inserted_timestamp
    FROM
        all_staked
    UNION ALL
    SELECT
        A.block_id,
        block_timestamp,
        address,
        b.lock_id,
        -1 * b.amount,
        b.currency,
        b.decimal,
        _inserted_timestamp
    FROM
        undel_base A
        LEFT JOIN unpool_lock_val C
        ON A.tx_id = C.tx_id
        JOIN undel_bal b
        ON COALESCE(
            A.lock_id,
            C.lock_id
        ) = b.lock_id
)
SELECT
    block_id,
    block_timestamp,
    'locked liquidity' AS balance_type,
    address,
    lock_id,
    currency,
    DECIMAL,
    SUM(amount) over(
        PARTITION BY address,
        currency,
        lock_id
        ORDER BY
            block_timestamp ASC rows unbounded preceding
    ) AS balance,
    concat_ws(
        '-',
        block_id,
        address,
        lock_id,
        currency
    ) AS _unique_key,
    _inserted_timestamp
FROM
    combine
