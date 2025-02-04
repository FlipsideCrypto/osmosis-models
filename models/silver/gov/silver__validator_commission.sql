{{ config(
    materialized = 'incremental',
    unique_key = ["tx_id","msg_group","msg_sub_group"],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['noncore'],
    enabled = false
) }}

WITH txs AS (

    SELECT
        DISTINCT A.tx_id,
        A.msg_group,
        msg_sub_group
    FROM
        {{ ref('silver__msg_attributes') }} A
    WHERE
        msg_type = 'withdraw_commission'
        AND tx_id <> '5D9503F5FE8FA7299099D88DF4989579D5FD0FCF7CB727DFE288A4BB03D2A00D'

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
msg_attributes_base AS (
    SELECT
        A.tx_id,
        A.block_id,
        A.block_timestamp,
        A.tx_succeeded,
        A.msg_type,
        A.msg_group,
        A.msg_sub_group,
        A.msg_index,
        A.attribute_key,
        A.attribute_value,
        A._inserted_timestamp
    FROM
        {{ ref('silver__msg_attributes') }} A
        JOIN txs b
        ON A.tx_id = b.tx_id
    WHERE
        (
            A.msg_group = b.msg_group
            AND A.msg_sub_group = b.msg_sub_group
            OR (
                A.msg_group IS NULL
                AND msg_type || attribute_key = 'txacc_seq'
            )
        )
        AND msg_type || attribute_key IN (
            'withdraw_commissionamount',
            'transferrecipient',
            'transferamount',
            'messagesender',
            'txacc_seq'
        )
        AND NOT (
            msg_type || attribute_key = 'messagesender'
            AND len(attribute_value) = 43
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
combo AS (
    SELECT
        tx_id,
        msg_group,
        msg_sub_group,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :sender :: STRING AS validator_address_operator,
        j :amount :: STRING AS amount
    FROM
        msg_attributes_base
    WHERE
        msg_type IN (
            'withdraw_commission',
            'message'
        )
    GROUP BY
        tx_id,
        msg_group,
        msg_sub_group
),
recipient_msg_index AS (
    SELECT
        A.tx_id,
        A.msg_group,
        A.msg_sub_group,
        A.msg_index
    FROM
        msg_attributes_base A
        JOIN combo b
        ON A.tx_id = b.tx_id
        AND A.msg_group = b.msg_group
        AND A.msg_sub_group = b.msg_sub_group
    WHERE
        A.msg_type = 'transfer'
        AND A.attribute_value = b.amount
),
recipient AS (
    SELECT
        A.tx_id,
        A.msg_group,
        A.msg_sub_group,
        A.attribute_value AS validator_address_reward
    FROM
        msg_attributes_base A
        JOIN recipient_msg_index b
        ON A.tx_id = b.tx_id
        AND A.msg_index = b.msg_index
    WHERE
        A.attribute_key = 'recipient'
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
        msg_attributes_base A
    WHERE
        attribute_key = 'acc_seq' qualify(ROW_NUMBER() over (PARTITION BY tx_id
    ORDER BY
        acc_seq_index) = 1)
),
block_tx_inserted AS (
    SELECT
        DISTINCT A.tx_id,
        A.block_id,
        A.block_timestamp,
        A.tx_succeeded,
        A._inserted_timestamp
    FROM
        msg_attributes_base A
)
SELECT
    b.block_id,
    b.block_timestamp,
    A.tx_id,
    b.tx_succeeded,
    C.tx_caller_address,
    A.msg_group,
    A.msg_sub_group,
    CASE
        WHEN am.value LIKE '%uosmo' THEN REPLACE(
            am.value,
            'uosmo'
        )
        WHEN am.value LIKE '%uion' THEN REPLACE(
            am.value,
            'uion'
        )
        WHEN am.value LIKE '%ibc%' THEN LEFT(am.value, CHARINDEX('i', am.value) -1)
    END :: INT AS amount,
    CASE
        WHEN am.value LIKE '%uosmo' THEN 'uosmo'
        WHEN am.value LIKE '%uion' THEN 'uion'
        WHEN am.value LIKE '%ibc%' THEN SUBSTRING(am.value, CHARINDEX('i', am.value), 99)
        ELSE 'uosmo'
    END AS currency,
    A.validator_address_operator,
    d.validator_address_reward,
    b._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['a.tx_id','a.msg_group','a.msg_sub_group']
    ) }} AS validator_commission_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    combo A
    JOIN LATERAL SPLIT_TO_TABLE(
        A.amount,
        ','
    ) am
    JOIN block_tx_inserted b
    ON A.tx_id = b.tx_id
    JOIN tx_address C
    ON A.tx_id = C.tx_id
    JOIN recipient d
    ON A.tx_id = d.tx_id
    AND A.msg_group = d.msg_group
    AND A.msg_sub_group = d.msg_sub_group
