{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    tags = ['noncore'],
    enabled = false
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
        AND COALESCE(
            A.msg_group,
            0
        ) = b.msg_group {# AND b.tx_grp_rn > 1 #}
    WHERE
        (
            A.msg_type IN (
                'superfluid_delegate',
                'lock_tokens',
                'add_tokens_to_lock'
            )
            OR (
                A.msg_type = 'tx'
                AND A.attribute_key = 'acc_seq'
            )
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
        {# A.msg_index,
        A.msg_type,
        #}
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
        {# A.msg_index,
        A.msg_type,
        #}
        A._inserted_timestamp
),
lper AS (
    SELECT
        tx_id,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS locker_address
    FROM
        base_msg_atts
    WHERE
        msg_type = 'tx' qualify ROW_NUMBER() over (
            PARTITION BY tx_id
            ORDER BY
                msg_index DESC
        ) = 1
),
FINAL AS (
    SELECT
        A.block_id,
        A.block_timestamp,
        A.tx_id,
        A.tx_succeeded,
        A.msg_group,
        {# A.msg_type, #}
        j :lock_id :: INT AS lock_id,
        j :validator :: STRING AS validator,
        A._INSERTED_TIMESTAMP
    FROM
        tx_msg_flat A
),
lock_amount AS (
    SELECT
        A.lock_id,
        A.currency,
        A.decimal,
        SUM(
            A.amount
        ) AS amount
    FROM
        {{ ref('silver__locked_liquidity_actions') }} A
        JOIN FINAL b
        ON A.lock_id = b.lock_id
    WHERE
        A.block_timestamp <= b.block_timestamp
    GROUP BY
        A.lock_id,
        A.currency,
        A.decimal
)
SELECT
    block_id,
    block_timestamp,
    A.tx_id,
    tx_succeeded,
    msg_group,
    'superfluid_delegate' AS msg_type,
    'convert' AS msg_action,
    'convert' AS msg_action_description,
    b.locker_address,
    A.lock_id,
    la.amount,
    la.currency,
    la.decimal,
    validator AS validator_address,
    TRUE AS is_superfluid,
    concat_ws(
        '-',
        A.tx_id,
        msg_group,
        COALESCE(
            A.lock_id,
            -1
        )
    ) AS _unique_key,
    _INSERTED_TIMESTAMP,
    {{ dbt_utils.generate_surrogate_key(
        ['_unique_key']
    ) }} AS locked_liquidity_actions_convert_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL A
    JOIN lper b
    ON A.tx_id = b.tx_id
    LEFT JOIN lock_amount la
    ON A.lock_id = la.lock_id
