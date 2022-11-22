{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE']
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
        A.blockchain,
        A.chain_id,
        A.tx_id,
        'SUCCEEDED' AS tx_status,
        A.msg_group,
        CASE
            WHEN A.msg_type = 'begin_unlock' THEN A.msg_index
        END AS msg_index,
        A.msg_type,
        A.attribute_key,
        A.attribute_value,
        NULL AS lock_id,
        A._inserted_timestamp
    FROM
        {{ ref('silver__msg_attributes') }} A
        JOIN (
            SELECT
                DISTINCT tx_id,
                msg_group
            FROM
                {{ ref('silver__locked_liquidity_transactions') }}
            UNION
            SELECT
                DISTINCT tx_id,
                msg_group
            FROM
                {{ ref('silver__superfluid_actions') }}
        ) b
        ON A.tx_id = b.tx_id
        AND A.msg_group = b.msg_group {# AND b.tx_grp_rn > 1 #}
    WHERE
        (
            (
                A.msg_type = 'message'
                AND A.attribute_key = 'action'
            )
            OR A.msg_type IN (
                'begin_unlock',
                'begin_unlock_all',
                'unbond',
                'unlock',
                'unlock_tokens'
            )
            AND attribute_key IN (
                'amount',
                'owner',
                'burner',
                'duration',
                'unlock_time',
                'sender',
                'denom',
                'period_lock_id',
                'lock_id'
            )
        ) --weird transactions that break the logic
        AND A.tx_id NOT IN (
            '523CBB1403A90A2A45A90ADFFC17F72100B99C286BD66DEDF22DD7F8A825127D',
            'B26B72516A670B4FFD31F4F7853E65F7463F7A46BDE61800DC17A41F55AB87A3',
            '34A6CEF2A87D6DB15DA1D7238D3A3BEABF8B4A1B460082B3C1F6C65DE5329CAC',
            '504A0BD295DA63E28D55BC7C46575C6C49D9C2612D0AF118BA2A33A089A25A6D',
            'B312127A7914D26444DA2C1104122F9CB7D3B50940F079544775C7EA4EE4981D',
            '413991DF25FF3A217BA42D84D811CABC4A580F12FA9A8BC204E45F22529185CB',
            '359F13D57A9B37DD6AC423A877FE66C9EB585679A0346037DC1B8DEC3B1F2B6D',
            '9A0FF4CC948097D2DE21A34F22C2CB2DAE2E354C31FF6899F378E9DB79CA765F'
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
        blockchain,
        chain_id,
        tx_id,
        tx_status,
        msg_group,
        msg_index,
        lock_id,
        _inserted_timestamp,
        LISTAGG(
            DISTINCT msg_type,
            '-'
        ) within GROUP (
            ORDER BY
                msg_type
        ) AS msg_type,
        {# lock_id, #}
        OBJECT_AGG(
            msg_type :: STRING || '--' || attribute_key :: STRING,
            attribute_value :: variant
        ) AS j
    FROM
        base_msg_atts
    WHERE
        msg_type <> 'message'
    GROUP BY
        block_id,
        block_timestamp,
        blockchain,
        chain_id,
        tx_id,
        tx_status,
        msg_group,
        msg_index,
        lock_id,
        _inserted_timestamp
),
FINAL AS (
    SELECT
        A.block_id,
        A.block_timestamp,
        A.blockchain,
        A.chain_id,
        A.tx_id,
        A.tx_status,
        A.msg_group,
        A.msg_type,
        COALESCE(
            j :"begin_unlock--period_lock_id",
            j :"begin_unlock_all--period_lock_id"
        ) :: INT AS lock_id,
        b.attribute_value AS action,
        CASE
            WHEN b.attribute_value = '/osmosis.lockup.MsgExtendLockup' THEN 'extend lockup'
            WHEN A.msg_type = 'unpool_pool_id' THEN 'unpool'
            WHEN j :"lock_tokens--duration" IS NOT NULL THEN 'initial lock'
            WHEN j :: STRING ILIKE '%unlock%'
            OR j :: STRING ILIKE '%undelegate%'
            OR j :: STRING ILIKE '%unbond%'
            OR action ILIKE '%undelegate%' THEN 'unlock-undelegate'
            WHEN j :: STRING LIKE '%add%'
            OR j :: STRING LIKE '%increase%' THEN 'add to position'
            WHEN j :: STRING ILIKE '%delegate%' THEN 'super upgrade'
        END hybrid_action,
        COALESCE(
            j :"add_tokens_to_lock--amount",
            j :"lock_tokens--amount",
            j :"superfluid_increase_delegation--amount",
            j :"burn--amount",
            j :"unpool_pool_id--denom"
        ) :: STRING AS amount,
        COALESCE(
            j :"add_tokens_to_lock--owner",
            j :"lock_tokens--owner",
            j :"begin_unlock--owner",
            j :"unlock--owner",
            j :"burn--burner",
            j :"unpool_pool_id--sender"
        ) :: STRING AS locker,
        COALESCE(
            j :"lock_tokens--duration",
            j :"begin_unlock--duration",
            j :"unlock--duration"
        ) :: STRING AS DURATION,
        NULLIF(
            COALESCE(
                j :"lock_tokens--unlock_time",
                j :"begin_unlock--unlock_time",
                j :"unlock--unlock_time"
            ) :: STRING,
            '0001-01-01 00:00:00 +0000 UTC'
        ) AS unlock_time,
        j :"unpool_pool_id--new_lock_ids" :: STRING AS new_lock_ids,
        A._INSERTED_TIMESTAMP
    FROM
        tx_msg_flat A
        JOIN base_msg_atts b
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
    WHERE
        A.msg_type = 'begin_unlock'
)
SELECT
    block_id,
    block_timestamp,
    blockchain,
    chain_id,
    tx_id,
    tx_status,
    msg_group,
    msg_type,
    lock_id,
    action,
    hybrid_action,
    amount,
    locker,
    DURATION,
    unlock_time,
    new_lock_ids,
    concat_ws(
        '-',
        tx_id,
        msg_group,
        COALESCE(
            lock_id,
            -1
        ),
        locker
    ) AS _unique_key,
    _INSERTED_TIMESTAMP
FROM
    FINAL
