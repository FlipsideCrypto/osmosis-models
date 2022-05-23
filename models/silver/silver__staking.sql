{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, msg_group, action, currency)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['_ingested_at::DATE'],
) }}

WITH base AS (

    SELECT
        A.tx_id,
        A.msg_type,
        A.msg_group,
        CASE
            WHEN LOWER(
                A.attribute_value
            ) = 'superfluid_delegate' THEN 'superfluid_delegate'
            WHEN LOWER(
                A.attribute_value
            ) = 'superfluid_undelegate' THEN 'superfluid_undelegate'
            WHEN LOWER(
                A.attribute_value
            ) = 'lock_and_superfluid_delegate' THEN 'lock_and_superfluid_delegate'
            WHEN LOWER(
                A.attribute_value
            ) = 'superfluid_unbond_underlying_lock' THEN 'superfluid_unbond_underlying_lock'
            WHEN LOWER(
                A.attribute_value
            ) LIKE '%redelegate%' THEN 'redelegate'
            WHEN LOWER(
                A.attribute_value
            ) LIKE '%undelegate%' THEN 'undelegate'
            WHEN LOWER(
                A.attribute_value
            ) LIKE '%delegate%' THEN 'delegate'
            WHEN LOWER(
                A.attribute_value
            ) LIKE '%withdraw%' THEN 'withdraw_rewards'
        END AS event_type
    FROM
        {{ ref('silver__msg_attributes') }} A
    WHERE
        attribute_key = 'action'
        AND (LOWER(attribute_value) LIKE '%dele%'
        OR attribute_value LIKE '%super%'
        OR attribute_value LIKE '%rewards%')
        AND attribute_value NOT IN (
            'superfluid_delegate',
            'superfluid_undelegate',
            'superfluid_unbond_underlying_lock'
        )

{% if is_incremental() %}
AND _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
),
msg_attr_base AS (
    SELECT
        A.tx_id,
        A.attribute_key,
        A.attribute_value,
        A.msg_index,
        A.msg_type,
        A.msg_group,
        conditional_change_event(
            CASE
                WHEN A.msg_type IN (
                    'delegate',
                    'redelegate',
                    'unbond',
                    'withdraw_rewards',
                    'claim'
                ) THEN 1
                ELSE 0
            END = 1
        ) over (
            PARTITION BY A.tx_id,
            A.msg_group
            ORDER BY
                A.msg_index
        ) AS change_index
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
        A.msg_type IN (
            'claim',
            'delegate',
            'message',
            'redelegate',
            'unbond',
            'withdraw_rewards',
            'tx',
            'transfer'
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
        b.event_Type,
        A.msg_group,
        A.change_index,
        COUNT(
            DISTINCT CASE
                WHEN A.msg_type = 'transfer' THEN msg_index
            END
        ) over(
            PARTITION BY A.tx_id,
            A.msg_group
        ) transfer_count_msg_group
    FROM
        msg_attr_base A
        JOIN base b
        ON A.tx_ID = b.tx_ID
        AND A.msg_group = b.msg_group
    WHERE
        A.msg_type IN (
            'claim',
            'delegate',
            'message',
            'redelegate',
            'unbond',
            'withdraw_rewards',
            'transfer'
        )
),
tx_address AS (
    SELECT
        A.tx_id,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS tx_caller_address
    FROM
        msg_attr_base A
    WHERE
        attribute_key = 'acc_seq'
),
valid AS (
    SELECT
        tx_id,
        msg_group,
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
        AND change_index > 0
    GROUP BY
        tx_id,
        msg_group
),
sendr AS (
    SELECT
        tx_id,
        msg_group,
        CASE
            WHEN event_Type IN (
                'delegate',
                'redelegate'
            )
            AND msg_type IN (
                'claim',
                'transfer'
            ) THEN 'claim'
            ELSE event_type
        END event_type,
        OBJECT_AGG(
            msg_type || '_' || attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :claim_sender :: STRING AS claim_sender,
        j :message_sender :: STRING AS message_sender,
        j :transfer_recipient :: STRING AS transfer_recipient
    FROM
        msg_attr A
    WHERE
        (
            (
                attribute_key = 'sender'
                AND msg_type <> 'transfer'
            )
            OR (
                msg_type = 'transfer'
                AND attribute_key = 'recipient'
                AND transfer_count_msg_group = 1
            )
        )
        AND (
            change_index > 0
            OR msg_type = 'transfer'
        )
    GROUP BY
        tx_id,
        msg_group,
        CASE
            WHEN event_Type IN (
                'delegate',
                'redelegate'
            )
            AND msg_type IN (
                'claim',
                'transfer'
            ) THEN 'claim'
            ELSE event_type
        END
),
amount AS (
    SELECT
        tx_id,
        msg_group,
        CASE
            WHEN event_Type IN (
                'delegate',
                'redelegate'
            )
            AND msg_type IN (
                'claim',
                'transfer'
            ) THEN 'claim'
            ELSE event_type
        END event_type,
        OBJECT_AGG(
            CASE
                WHEN msg_type = 'transfer' THEN 'transfer_'
                ELSE ''
            END || attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :amount :: STRING AS amount,
        j :transfer_amount :: STRING AS transfer_amount
    FROM
        msg_attr
    WHERE
        attribute_key = 'amount'
        AND (
            change_index > 0
            OR (
                msg_type = 'transfer'
                AND transfer_count_msg_group = 1
            )
        )
    GROUP BY
        tx_id,
        msg_group,
        CASE
            WHEN event_Type IN (
                'delegate',
                'redelegate'
            )
            AND msg_type IN (
                'claim',
                'transfer'
            ) THEN 'claim'
            ELSE event_type
        END
),
ctime AS (
    SELECT
        tx_id,
        msg_group,
        CASE
            WHEN event_Type IN (
                'delegate',
                'redelegate'
            )
            AND msg_type IN (
                'claim',
                'transfer'
            ) THEN 'claim'
            ELSE event_type
        END event_type,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :completion_time :: STRING AS completion_time
    FROM
        msg_attr
    WHERE
        attribute_key = 'completion_time'
        AND change_index > 0
    GROUP BY
        tx_id,
        msg_group,
        CASE
            WHEN event_Type IN (
                'delegate',
                'redelegate'
            )
            AND msg_type IN (
                'claim',
                'transfer'
            ) THEN 'claim'
            ELSE event_type
        END
),
prefinal AS (
    SELECT
        A.tx_ID,
        A.msg_group,
        A.event_type AS action,
        COALESCE(
            b.claim_sender,
            b.transfer_recipient,
            b.message_sender
        ) AS delegator_address,
        COALESCE(
            d.amount,
            d.transfer_amount
        ) AS amount,
        C.validator_address,
        C.redelegate_source_validator_address,
        e.completion_time
    FROM
        (
            SELECT
                DISTINCT tx_id,
                msg_group,
                CASE
                    WHEN event_Type IN (
                        'delegate',
                        'redelegate'
                    )
                    AND msg_type IN (
                        'claim',
                        'transfer'
                    ) THEN 'claim'
                    ELSE event_type
                END event_type
            FROM
                msg_attr
        ) A
        JOIN sendr b
        ON A.tx_ID = b.tx_ID
        AND A.msg_group = b.msg_group
        AND A.event_type = b.event_type
        JOIN valid C
        ON A.tx_ID = C.tx_ID
        AND A.msg_group = C.msg_group
        JOIN amount d
        ON A.tx_ID = d.tx_ID
        AND A.msg_group = d.msg_group
        AND A.event_type = d.event_type
        LEFT JOIN ctime e
        ON A.tx_ID = e.tx_ID
        AND A.msg_group = e.msg_group
        AND A.event_type = e.event_type
)
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
    A.delegator_address,CASE
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
    END :: INT AS amount,CASE
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
