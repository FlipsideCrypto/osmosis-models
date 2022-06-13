{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, msg_group, action, currency, delegator_address, validator_address)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH base AS (

    SELECT
        A.tx_id,
        A.msg_type,
        A.msg_index,
        msg_group
    FROM
        {{ ref('silver__msg_attributes') }} A
    WHERE
        msg_type IN (
            'delegate',
            'redelegate',
            'unbond'
        )
        AND attribute_value NOT IN (
            'superfluid_delegate',
            'superfluid_undelegate',
            'superfluid_unbond_underlying_lock'
        )

{% if is_incremental() %}
AND _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
),
msg_attr AS (
    SELECT
        A.tx_id,
        A.attribute_key,
        A.attribute_value,
        A.msg_index,
        A.msg_type,
        A.msg_group
    FROM
        {{ ref('silver__msg_attributes') }} A
        JOIN (
            SELECT
                DISTINCT tx_id,
                msg_index
            FROM
                base
            UNION ALL
            SELECT
                DISTINCT tx_id,
                msg_index + 1 msg_index
            FROM
                base
        ) b
        ON A.tx_ID = b.tx_ID
        AND A.msg_index = b.msg_index
    WHERE
        A.msg_type IN (
            'delegate',
            'message',
            'redelegate',
            'unbond'
        )

{% if is_incremental() %}
AND _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
),
tx_address AS (
    SELECT
        A.tx_id,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        SPLIT_PART(
            j :acc_seq :: STRING,
            '/',
            0
        ) AS tx_caller_address
    FROM
        {{ ref('silver__msg_attributes') }} A
        JOIN (
            SELECT
                DISTINCT tx_id
            FROM
                base
        ) b
        ON A.tx_ID = b.tx_ID
    WHERE
        attribute_key = 'acc_seq'

{% if is_incremental() %}
AND _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
GROUP BY
    A.tx_id,
    msg_group
),
valid AS (
    SELECT
        tx_id,
        msg_group,
        msg_index,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        COALESCE(
            j :validator :: STRING,
            j :destination_validator :: STRING
        ) AS validator_address,
        j :source_validator :: STRING AS redelegate_source_validator_address
    FROM
        msg_attr
    WHERE
        attribute_key LIKE '%validator'
    GROUP BY
        tx_id,
        msg_group,
        msg_index
),
sendr AS (
    SELECT
        tx_id,
        msg_group,
        msg_index,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :sender :: STRING AS sender
    FROM
        msg_attr A
    WHERE
        attribute_key = 'sender'
    GROUP BY
        tx_id,
        msg_group,
        msg_index
),
amount AS (
    SELECT
        tx_id,
        msg_group,
        msg_index,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :amount :: STRING AS amount
    FROM
        msg_attr
    WHERE
        attribute_key = 'amount'
    GROUP BY
        tx_id,
        msg_group,
        msg_index
),
ctime AS (
    SELECT
        tx_id,
        msg_group,
        msg_index,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :completion_time :: STRING AS completion_time
    FROM
        msg_attr
    WHERE
        attribute_key = 'completion_time'
    GROUP BY
        tx_id,
        msg_group,
        msg_index
),
prefinal AS (
    SELECT
        A.tx_ID,
        A.msg_group,
        b.sender AS delegator_address,
        d.amount,
        A.msg_type AS action,
        C.validator_address,
        C.redelegate_source_validator_address,
        e.completion_time
    FROM
        (
            SELECT
                DISTINCT tx_id,
                msg_group,
                msg_index,
                REPLACE(
                    msg_type,
                    'unbond',
                    'undelegate'
                ) msg_type
            FROM
                base
        ) A
        JOIN sendr b
        ON A.tx_ID = b.tx_ID
        AND A.msg_group = b.msg_group
        AND A.msg_index + 1 = b.msg_index
        JOIN valid C
        ON A.tx_ID = C.tx_ID
        AND A.msg_group = C.msg_group
        AND A.msg_index = C.msg_index
        JOIN amount d
        ON A.tx_ID = d.tx_ID
        AND A.msg_group = d.msg_group
        AND A.msg_index = d.msg_index
        LEFT JOIN ctime e
        ON A.tx_ID = e.tx_ID
        AND A.msg_group = e.msg_group
        AND A.msg_index = e.msg_index
),
add_dec AS (
    SELECT
        b.block_id,
        b.block_timestamp,
        b.blockchain,
        b.chain_id,
        A.tx_id,
        b.tx_status,
        C.tx_caller_address,
        A.action,
        A.msg_group,
        A.delegator_address,
        SUM(
            CASE
                WHEN A.split_amount LIKE '%uosmo' THEN REPLACE(
                    A.split_amount,
                    'uosmo'
                )
                WHEN A.split_amount LIKE '%uion' THEN REPLACE(
                    A.split_amount,
                    'uion'
                )
                WHEN A.split_amount LIKE '%pool%' THEN LEFT(A.split_amount, CHARINDEX('g', A.split_amount) -1)
                WHEN A.split_amount LIKE '%ibc%' THEN LEFT(A.split_amount, CHARINDEX('i', A.split_amount) -1)
                ELSE A.split_amount
            END :: INT
        ) AS amount,
        CASE
            WHEN A.split_amount LIKE '%uosmo' THEN 'uosmo'
            WHEN A.split_amount LIKE '%uion' THEN 'uion'
            WHEN A.split_amount LIKE '%pool%' THEN SUBSTRING(A.split_amount, CHARINDEX('g', A.split_amount), 99)
            WHEN A.split_amount LIKE '%ibc%' THEN SUBSTRING(A.split_amount, CHARINDEX('i', A.split_amount), 99)
            ELSE 'uosmo'
        END AS currency,
        A.validator_address,
        A.redelegate_source_validator_address,
        A.completion_time :: datetime completion_time,
        b._INGESTED_AT
    FROM
        (
            SELECT
                p.tx_Id,
                p.action,
                p.msg_group,
                p.delegator_address,
                p.validator_address,
                p.redelegate_source_validator_address,
                p.completion_time,
                am.value AS split_amount
            FROM
                prefinal p,
                LATERAL SPLIT_TO_TABLE(
                    p.amount,
                    ','
                ) am
        ) A
        JOIN (
            SELECT
                tx_ID,
                block_id,
                block_timestamp,
                blockchain,
                chain_id,
                tx_status,
                _INGESTED_AT
            FROM
                {{ ref('silver__transactions') }}

{% if is_incremental() %}
WHERE
    _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
) b
ON A.tx_Id = b.tx_ID
JOIN tx_address C
ON A.tx_id = C.tx_id
GROUP BY
    b.block_id,
    b.block_timestamp,
    b.blockchain,
    b.chain_id,
    A.tx_id,
    b.tx_status,
    C.tx_caller_address,
    A.action,
    A.msg_group,
    A.delegator_address,
    currency,
    A.validator_address,
    A.redelegate_source_validator_address,
    completion_time,
    b._INGESTED_AT
)
SELECT
    block_id,
    A.block_timestamp,
    A.blockchain,
    A.chain_id,
    A.tx_id,
    A.tx_status,
    A.tx_caller_address,
    A.action,
    A.msg_group,
    A.delegator_address,
    A.amount,
    A.currency,
    A.validator_address,
    A.redelegate_source_validator_address,
    A.completion_time,
    amd.raw_metadata [1] :exponent :: INT AS DECIMAL,
    A._INGESTED_AT
FROM
    add_dec A
    LEFT OUTER JOIN {{ ref('silver__asset_metadata') }}
    amd
    ON A.currency = amd.address
