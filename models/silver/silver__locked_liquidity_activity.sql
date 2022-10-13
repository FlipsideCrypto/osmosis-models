{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge'
) }}

WITH base_msg_atts AS (

    SELECT
        A.tx_id,
        A.block_timestamp,
        A.msg_group,
        {# A.msg_sub_group,
        A.msg_index,
        #}
        {# msg_index, #}
        A.msg_type,
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
        LEFT JOIN {{ ref('silver__superfluid_actions') }} C
        ON A.tx_id = C.tx_id
        AND A.msg_group = C.msg_group
    WHERE
        COALESCE(
            b.lock_id,
            C.lock_id
        ) IS NOT NULL
        AND (
            (
                A.msg_type = 'message'
                AND A.attribute_key = 'action'
            )
            OR A.msg_type IN (
                'add_tokens_to_lock',
                'begin_unlock',
                'begin_unlock_all',
                'burn',
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
                'unlock_time'
            )
        )
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
        MAX(
            _inserted_timestamp
        )
    FROM
        max_date
)
{% endif %}
),
tx_msg_flat AS (
    SELECT
        tx_id,
        block_timestamp,
        msg_group,
        LISTAGG(
            DISTINCT msg_type,
            '-'
        ) AS msg_type,
        lock_id,
        OBJECT_AGG(
            msg_type :: STRING || '--' || attribute_key :: STRING,
            attribute_value :: variant
        ) AS j
    FROM
        base_msg_atts
    WHERE
        msg_type <> 'message'
    GROUP BY
        tx_id,
        block_timestamp,
        msg_group,
        {# msg_index, #}
        {# msg_type, #}
        lock_id
) {# ,
tx_body AS (
    SELECT
        A.tx_id,
        msg_type,
        msg_group,
        msg :sender :: STRING AS delegator_address,
        msg :coins [0] :amount :: INT AS amount,
        msg :coins [0] :denom :: STRING AS currency
    FROM
        {{ ref('silver__tx_body_msgs') }} A
        JOIN base_msg_atts b
        ON A.tx_id = b.tx_id
        AND A.msg_group = b.msg_group
) #}
SELECT
    A.tx_id,
    A.block_timestamp,
    A.msg_group,
    {# msg_index, #}
    A.msg_type,
    A.lock_id,
    j,
    b.attribute_value AS action,
    CASE
        WHEN j :"lock_tokens--duration" IS NOT NULL THEN 'inital lock'
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
        j :"burn--amount"
    ) :: STRING AS amount,
    COALESCE(
        j :"add_tokens_to_lock--owner",
        j :"lock_tokens--owner"
    ) :: STRING AS locker,
    j :"lock_tokens--duration" :: STRING AS DURATION,
    j :"ock_tokens--unlock_time" :: STRING AS unlock_time
FROM
    tx_msg_flat A
    LEFT JOIN base_msg_atts b
    ON A.tx_id = b.tx_id
    AND A.msg_group = b.msg_group
    AND A.lock_id = b.lock_id
    AND b.msg_type = 'message'
