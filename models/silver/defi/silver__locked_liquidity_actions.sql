{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    tags = ['noncore']
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

base_msg_atts AS (
    SELECT
        DISTINCT A.block_id,
        A.block_timestamp,
        A.tx_id,
        TRUE AS tx_succeeded,
        A.msg_group,
        CASE
            WHEN A.msg_type = 'message' THEN A.msg_type
            ELSE 'not'
        END msg_type,
        A.attribute_key,
        A.attribute_value,
        COALESCE(
            b.lock_id :: INT,
            C.lock_id :: INT
        ) AS lock_id,
        A._inserted_timestamp
    FROM
        {{ ref('silver__msg_attributes') }} A
        LEFT JOIN {{ ref('silver__locked_liquidity_transactions') }}
        b
        ON A.tx_id = b.tx_id
        AND A.msg_group = b.msg_group
        AND (
            b.id_count = 1
            OR (
                b.id_count > 1
                AND A.msg_index = b.msg_index
            )
        )
        LEFT JOIN {{ ref('silver__superfluid_actions') }} C
        ON A.tx_id = C.tx_id
        AND A.msg_group = C.msg_group
    WHERE
        COALESCE(
            b.tx_id,
            C.tx_id
        ) IS NOT NULL
        AND (
            (
                A.msg_type = 'message'
                AND A.attribute_key = 'action'
            )
            OR A.msg_type IN (
                'add_tokens_to_lock',
                'lock_tokens',
                'superfluid_delegate',
                'superfluid_increase_delegation',
                'superfluid_unbond_lock',
                'superfluid_undelegate',
                'unbond',
                'unlock',
                'unlock_tokens',
                'unpool_pool_id'
            )
            AND attribute_key IN (
                'amount',
                'owner',
                'burner',
                'duration',
                'unlock_time',
                'sender',
                'denom',
                'new_lock_ids'
            )
        ) -- weird transactions that break THE logic
        AND A.tx_id NOT IN (
            '523CBB1403A90A2A45A90ADFFC17F72100B99C286BD66DEDF22DD7F8A825127D',
            'B26B72516A670B4FFD31F4F7853E65F7463F7A46BDE61800DC17A41F55AB87A3',
            '34A6CEF2A87D6DB15DA1D7238D3A3BEABF8B4A1B460082B3C1F6C65DE5329CAC',
            '504A0BD295DA63E28D55BC7C46575C6C49D9C2612D0AF118BA2A33A089A25A6D',
            'B312127A7914D26444DA2C1104122F9CB7D3B50940F079544775C7EA4EE4981D',
            '413991DF25FF3A217BA42D84D811CABC4A580F12FA9A8BC204E45F22529185CB'
        )

{% if is_incremental() %}
AND A._inserted_timestamp >= (
    SELECT
        _inserted_timestamp
    FROM
        max_date
)
{% endif %}
),
tx_msg_flat AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        msg_group,
        _inserted_timestamp,
        LISTAGG(
            DISTINCT msg_type,
            '-'
        ) within GROUP (
            ORDER BY
                msg_type
        ) AS msg_type,
        lock_id,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j
    FROM
        base_msg_atts
    WHERE
        msg_type <> 'message'
    GROUP BY
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        msg_group,
        lock_id,
        _inserted_timestamp
),
msg_based AS (
    SELECT
        A.block_id,
        A.block_timestamp,
        A.tx_id,
        A.tx_succeeded,
        A.msg_group,
        A.msg_type,
        A.lock_id,
        b.attribute_value AS action,
        NULL AS hybrid_action,
        COALESCE(
            j :"amount",
            j :"denom"
        ) :: STRING AS amount,
        COALESCE(
            j :"owner",
            j :"burner",
            j :"sender"
        ) :: STRING AS locker,
        j :"duration" :: STRING AS DURATION,
        NULLIF(
            j :"unlock_time" :: STRING,
            '0001-01-01 00:00:00 +0000 UTC'
        ) AS unlock_time,
        j :"new_lock_ids" :: STRING AS new_lock_ids,
        A._INSERTED_TIMESTAMP
    FROM
        tx_msg_flat A
        LEFT JOIN base_msg_atts b
        ON A.tx_id = b.tx_id
        AND A.msg_group = b.msg_group
        AND COALESCE(
            A.lock_id,
            -1
        ) = COALESCE(
            b.lock_id,
            -1
        )
        AND b.msg_type = 'message'
),
combo_with_super_undel AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        msg_group,
        msg_type,
        lock_id,
        action,
        hybrid_action,
        amount :: STRING AS amount,
        locker,
        DURATION,
        unlock_time,
        new_lock_ids,
        _INSERTED_TIMESTAMP
    FROM
        msg_based
    UNION ALL
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        msg_group,
        msg_type,
        lock_id,
        action,
        hybrid_action,
        amount :: STRING AS amount,
        locker,
        DURATION,
        unlock_time,
        new_lock_ids,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__locked_liquidity_actions_begin_unlock') }}
    UNION ALL
    SELECT
        A.block_id,
        A.block_timestamp,
        A.tx_id,
        A.tx_succeeded,
        A.msg_group,
        A.msg_type,
        A.lock_id,
        'unlock' AS action,
        'unlock' AS hybrid_action,
        NULL :: STRING AS amount,
        delegator_address AS locker,
        NULL :: STRING AS DURATION,
        NULL :: STRING AS unlock_time,
        NULL :: STRING AS new_lock_ids,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__superfluid_actions') }} A
        LEFT JOIN (
            SELECT
                DISTINCT tx_id,
                lock_id
            FROM
                {{ ref('silver__locked_liquidity_actions_begin_unlock') }}
        ) b
        ON A.tx_id = b.tx_id
        AND A.lock_id = b.lock_id
    WHERE
        msg_type = '/osmosis.superfluid.MsgSuperfluidUndelegate'
        AND b.tx_id IS NULL
),
tx_body AS (
    SELECT
        tx_id,
        tx_succeeded,
        msg_type,
        msg_group,
        delegator_address,
        amount,
        validator_address,
        lock_id,
        pool_id
    FROM
        {{ ref('silver__superfluid_actions') }} A

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            _inserted_timestamp
        FROM
            max_date
    )
{% endif %}
),
all_super_pools AS (
    SELECT
        lock_id,
        MIN(block_id) block_id
    FROM
        {{ ref('silver__superfluid_actions') }}
    GROUP BY
        lock_id
),
fin AS (
    SELECT
        A.block_id,
        A.block_timestamp,
        A.tx_id,
        A.tx_succeeded,
        A.msg_group,
        COALESCE(NULLIF(b.msg_type, 'not'), A.msg_type) msg_type,
        COALESCE(
            A.action,
            A.msg_type
        ) AS msg_action,
        A.hybrid_action AS msg_action_description,
        A.locker AS locker_address,
        A.lock_id,
        CASE
            WHEN A.amount LIKE 'gamm%' THEN NULL
            WHEN A.amount LIKE '%uosmo' THEN REPLACE(
                A.amount,
                'uosmo'
            )
            ELSE SPLIT_PART(
                TRIM(
                    REGEXP_REPLACE(
                        A.amount,
                        '[^[:digit:]]',
                        ' '
                    )
                ),
                ' ',
                0
            )
        END :: INT AS amount,
        CASE
            WHEN A.amount LIKE 'gamm%' THEN A.amount
            WHEN A.amount LIKE '%uosmo' THEN 'uosmo'
            ELSE RIGHT(A.amount, LENGTH(A.amount) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(A.amount, '[^[:digit:]]', ' ')), ' ', 0)))
        END AS currency,
        CASE
            WHEN A.amount LIKE '%uosmo' THEN 6
            ELSE 18
        END AS DECIMAL,
        COALESCE(
            b.pool_id,
            CASE
                WHEN A.amount LIKE '%pool%' THEN RIGHT(A.amount, len(A.amount) - POSITION('pool', A.amount) -4)
            END :: INT
        ) AS pool_id,
        A.duration AS lock_duration,
        A.unlock_time AS unlock_time,
        CASE
            WHEN (
                b.tx_id IS NOT NULL
                OR C.lock_id IS NOT NULL
            )
            AND A.block_id >= C.block_id THEN TRUE
            WHEN COALESCE(
                A.action,
                A.msg_type
            ) ILIKE '%superfluid%' THEN TRUE
            ELSE FALSE
        END is_superfluid,
        A.new_lock_ids AS unpool_new_lock_ids,
        concat_ws(
            '-',
            A.tx_id,
            A.msg_group,
            COALESCE(
                A.lock_id,
                -1
            ),
            A.locker
        ) AS _unique_key,
        A._INSERTED_TIMESTAMP
    FROM
        combo_with_super_undel A
        LEFT JOIN tx_body b
        ON A.tx_id = b.tx_id
        AND A.msg_group = b.msg_group
        LEFT JOIN all_super_pools C
        ON A.lock_id = C.lock_id
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    msg_group,
    msg_type,
    msg_action,
    CASE
        WHEN msg_action_description IS NOT NULL THEN msg_action_description
        WHEN msg_action = '/osmosis.lockup.MsgExtendLockup' THEN 'extend lockup'
        WHEN msg_type = '/osmosis.superfluid.MsgUnPoolWhitelistedPool'
        OR msg_action = 'unpool_whitelisted_pool' THEN 'unpool'
        WHEN lock_duration IS NULL
        AND unlock_time IS NULL THEN 'add to position'
        WHEN lock_duration IS NOT NULL
        AND unlock_time IS NULL THEN 'initial lock'
        WHEN msg_action = 'unlock_period_lock' THEN 'unlock-undelegate'
    END AS msg_action_description,
    locker_address,
    lock_id,
    amount,
    currency,
    DECIMAL,
    pool_id,
    lock_duration,
    unlock_time,
    is_superfluid,
    unpool_new_lock_ids,
    _unique_key,
    _INSERTED_TIMESTAMP,
    {{ dbt_utils.generate_surrogate_key(
        ['_unique_key']
    ) }} AS locked_liquidity_actions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    fin
WHERE
    lock_id IS NOT NULL
