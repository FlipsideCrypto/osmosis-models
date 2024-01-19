{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['noncore']
) }}

WITH msg_attributes_cte AS (

    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        msg_type,
        msg_group,
        msg_index,
        attribute_key,
        attribute_value,
        _inserted_timestamp
    FROM
        {{ ref('silver__msg_attributes') }} A
    WHERE
        block_id >= 12834101
        AND block_timestamp :: DATE >= '2023-12-18'
        AND (
            msg_type = 'withdraw_rewards'
            OR (
                msg_type = 'tx'
                AND attribute_key = 'acc_seq'
            )
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
),
with_agg AS (
    SELECT
        block_id,
        block_timestamp,
        tx_succeeded,
        tx_id,
        msg_type,
        msg_group,
        msg_index,
        _inserted_timestamp,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :validator :: STRING AS validator_address,
        j :amount :: STRING AS amount_raw,
        j :delegator :: STRING AS delegator_address,
        SPLIT_PART(
            TRIM(
                REGEXP_REPLACE(
                    amount_raw,
                    '[^[:digit:]]',
                    ' '
                )
            ),
            ' ',
            0
        ) AS amount_INT,
        RIGHT(amount_raw, LENGTH(amount_raw) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(amount_raw, '[^[:digit:]]', ' ')), ' ', 0))) AS currency
    FROM
        msg_attributes_cte
    WHERE
        msg_type = 'withdraw_rewards'
    GROUP BY
        block_id,
        block_timestamp,
        tx_succeeded,
        tx_id,
        msg_type,
        msg_group,
        msg_index,
        _inserted_timestamp
),
tx_address AS (
    SELECT
        A.tx_id,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS tx_caller_address,
        SPLIT_PART(
            attribute_value,
            '/',
            1
        ) AS acc_seq_index
    FROM
        msg_attributes_cte A
    WHERE
        attribute_key = 'acc_seq' qualify(ROW_NUMBER() over (PARTITION BY tx_id
    ORDER BY
        acc_seq_index) = 1)
)
SELECT
    A.block_id,
    A.block_timestamp,
    A.tx_id,
    A.tx_succeeded,
    tx.tx_caller_address,
    'withdraw_rewards' AS action,
    A.msg_group,
    A.delegator_address,
    A.amount_int :: INT AS amount,
    A.currency,
    A.validator_address,
    amd.decimal AS DECIMAL,
    A._inserted_timestamp,
    concat_ws(
        '-',
        A.tx_id,
        A.msg_index
    ) AS _unique_key,
    {{ dbt_utils.generate_surrogate_key(
        ['a.tx_id','msg_index']
    ) }} AS staking_rewards_2_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    with_agg A
    JOIN tx_address tx
    ON A.tx_id = tx.tx_id
    LEFT OUTER JOIN {{ ref('silver__asset_metadata') }}
    amd
    ON A.currency = amd.address
