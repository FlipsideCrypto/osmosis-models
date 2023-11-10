{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['noncore']
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
            'unlock-undelegate'
        )
        AND (
            unlock_time IS NOT NULL
            OR msg_action = '/osmosis.superfluid.MsgSuperfluidUndelegate'
            OR msg_type = '/osmosis.superfluid.MsgSuperfluidUndelegate'
        )
        AND tx_id <> '1A2A80A7112995EA1A22D6EDBFB26AE7B863852A9E7F59779247F8928DA218D6'

{% if is_incremental() %}
AND block_timestamp :: DATE >=(
    SELECT
        DATEADD('day', -2, MAX(block_timestamp))
    FROM
        {{ this }})
    {% endif %}
),
unpool_base AS (
    SELECT
        block_id,
        block_timestamp,
        locker_address AS address,
        currency,
        DECIMAL,
        msg_action_description,
        tx_id,
        COALESCE(LAG(block_id) over (
    ORDER BY
        block_id), 1) AS lower_bound_unpool,
        _inserted_timestamp
    FROM
        {{ ref('silver__locked_liquidity_actions') }}
    WHERE
        msg_action_description = 'unpool'

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
        A.locker_address AS address,
        A.currency,
        A.decimal,
        b.block_id,
        b.tx_id,
        b.block_timestamp,
        b._inserted_timestamp,
        SUM(
            CASE
                WHEN A.msg_action_description IN (
                    'initial lock',
                    'add to position'
                ) THEN 1
                ELSE -1
            END * amount
        ) AS unpool_amount
    FROM
        {{ ref('silver__locked_liquidity_actions') }} A
        JOIN unpool_base b
        ON A.locker_address = b.address
        AND A.currency = b.currency
        AND A.block_id < b.block_id
        AND A.block_id > lower_bound_unpool
        LEFT JOIN undel_base C
        ON A.locker_address = C.address
        AND A.lock_id = C.lock_id
    WHERE
        A.lock_id IS NOT NULL
        AND C.tx_id IS NULL
    GROUP BY
        A.lock_id,
        A.locker_address,
        A.currency,
        A.decimal,
        b.block_id,
        b.tx_id,
        b.block_timestamp,
        b._inserted_timestamp
),
undel_bal AS (
    SELECT
        A.lock_id,
        A.currency,
        A.decimal,
        SUM(COALESCE(amount, 0)) amount
    FROM
        {{ ref('silver__locked_liquidity_actions') }} A
        JOIN undel_base b
        ON A.lock_id = b.lock_id
    WHERE
        A.amount > 0
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
        -1 * amount,
        b.currency,
        b.decimal,
        _inserted_timestamp
    FROM
        undel_base A
        JOIN undel_bal b
        ON A.lock_id = b.lock_id
    UNION ALL
    SELECT
        block_id,
        block_timestamp,
        address,
        lock_id,
        -1 * unpool_amount,
        currency,
        DECIMAL,
        _inserted_timestamp
    FROM
        unpool_lock_val A
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
