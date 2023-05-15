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
        A.tx_id,
        TRUE AS tx_succeeded,
        A.msg_group,
        A.msg_index,
        A.msg_type,
        A.attribute_key,
        A.attribute_value,
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
        A.msg_type IN (
            'superfluid_delegate',
            'lock_tokens',
            'add_tokens_to_lock'
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
        A.block_id,
        A.block_timestamp,
        A.tx_id,
        A.tx_succeeded,
        A.msg_group,
        A.msg_index,
        A.msg_type,
        A._inserted_timestamp,
        OBJECT_AGG(
            A.attribute_key :: STRING,
            A.attribute_value :: variant
        ) AS j
    FROM
        base_msg_atts A
        JOIN (
            SELECT
                DISTINCT tx_id,
                msg_group
            FROM
                base_msg_atts
            WHERE
                msg_type = 'superfluid_delegate'
            EXCEPT
            SELECT
                tx_id,
                msg_group
            FROM
                base_msg_atts
            WHERE
                msg_type IN (
                    'lock_tokens',
                    'add_tokens_to_lock'
                )
        ) b
        ON A.tx_id = b.tx_id
        AND A.msg_group = b.msg_group
    GROUP BY
        A.block_id,
        A.block_timestamp,
        A.tx_id,
        A.tx_succeeded,
        A.msg_group,
        A.msg_index,
        A.msg_type,
        A._inserted_timestamp
),
FINAL AS (
    SELECT
        A.block_id,
        A.block_timestamp,
        A.tx_id,
        A.tx_succeeded,
        A.msg_group,
        A.msg_type,
        j :lock_id :: INT AS lock_id,
        j :validator :: STRING AS validator,
        {# COALESCE(
        j :"add_tokens_to_lock--owner",
        j :"lock_tokens--owner",
        j :"begin_unlock--owner",
        j :"unlock--owner",
        j :"burn--burner",
        j :"unpool_pool_id--sender"
) :: STRING AS locker,
#}
j,
A._INSERTED_TIMESTAMP
FROM
    tx_msg_flat A
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    msg_group,
    msg_type,
    lock_id,
    validator,
    j,
    concat_ws(
        '-',
        tx_id,
        msg_group,
        COALESCE(
            lock_id,
            -1
        )
    ) AS _unique_key,
    _INSERTED_TIMESTAMP
FROM
    FINAL
