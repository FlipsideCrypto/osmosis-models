{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, msg_group, currency)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['_ingested_at::DATE'],
) }}

WITH base AS (

    SELECT
        A.block_id,
        A.block_timestamp,
        A.blockchain,
        A.chain_id,
        A.tx_id,// A.tx_status,
        A._Ingested_at,
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
        END AS event_Type
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
msg_attr AS (
    SELECT
        A.tx_id,
        attribute_key,
        attribute_value,
        msg_index,
        msg_type,
        b.event_Type,
        A.msg_group
    FROM
        {{ ref('silver__msg_attributes') }} A
        JOIN (
            SELECT
                tx_id,
                msg_group,
                event_type
            FROM
                base
        ) b
        ON A.tx_ID = b.tx_ID
        AND A.msg_group = b.msg_group
    WHERE
        msg_type IN (
            'claim',
            'delegate',
            'message',
            'redelegate',
            'unbond',
            'withdraw_rewards'
        )

{% if is_incremental() %}
AND _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
),
tx_address AS (
    SELECT
        tx_id,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS tx_caller_address
    FROM
        {{ ref('silver__msg_attributes') }} A
    WHERE
        tx_id IN (
            SELECT
                tx_id
            FROM
                base
        )
        AND attribute_key = 'acc_seq'

{% if is_incremental() %}
AND _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
),
prefinal AS (
    SELECT
        'delegate' AS action,
        tx_ID,
        msg_group,
        "'sender'" :: STRING AS delegator_address,
        "'amount'" AS amount,
        "'validator'" AS validator_address,
        NULL AS redelegate_source_validator_address,
        NULL AS completion_time
    FROM
        (
            SELECT
                A.tx_ID,
                A.attribute_value,
                A.attribute_key,
                msg_group
            FROM
                msg_attr A
                LEFT JOIN (
                    SELECT
                        tx_ID,
                        msg_index
                    FROM
                        msg_attr
                    WHERE
                        event_Type = 'delegate'
                ) b
                ON A.tx_ID = b.tx_ID
                AND A.msg_index = b.msg_index
                LEFT JOIN (
                    SELECT
                        tx_ID,
                        msg_index + 1 AS msg_index
                    FROM
                        msg_attr
                    WHERE
                        event_Type = 'delegate'
                ) C
                ON A.tx_ID = C.tx_ID
                AND A.msg_index = C.msg_index
            WHERE
                (
                    b.tx_ID IS NOT NULL
                    OR C.tx_ID IS NOT NULL
                )
                AND event_Type = 'delegate'
                AND attribute_key IN (
                    'sender',
                    'validator',
                    'amount'
                )
        ) x pivot(MAX(attribute_value) for attribute_key IN ('amount', 'validator', 'sender')) AS p
    UNION ALL
    SELECT
        'redelegate' AS action,
        tx_ID,
        msg_group,
        "'sender'" :: STRING AS delegator_address,
        "'amount'" AS amount,
        "'destination_validator'" AS validator_address,
        "'source_validator'" AS redelegate_source_validator_address,
        "'completion_time'" AS completion_time
    FROM
        (
            SELECT
                A.tx_ID,
                A.attribute_value,
                A.attribute_key,
                msg_group
            FROM
                msg_attr A
                LEFT JOIN (
                    SELECT
                        tx_ID,
                        msg_index
                    FROM
                        msg_attr
                    WHERE
                        event_Type = 'redelegate'
                ) b
                ON A.tx_ID = b.tx_ID
                AND A.msg_index = b.msg_index
                LEFT JOIN (
                    SELECT
                        tx_ID,
                        msg_index + 1 AS msg_index
                    FROM
                        msg_attr
                    WHERE
                        event_Type = 'redelegate'
                ) C
                ON A.tx_ID = C.tx_ID
                AND A.msg_index = C.msg_index
            WHERE
                (
                    b.tx_ID IS NOT NULL
                    OR C.tx_ID IS NOT NULL
                )
                AND event_Type = 'redelegate'
                AND attribute_key IN (
                    'sender',
                    'source_validator',
                    'destination_validator',
                    'amount',
                    'completion_time'
                )
        ) x pivot(MAX(attribute_value) for attribute_key IN ('sender', 'source_validator', 'destination_validator', 'amount', 'completion_time')) AS p
    UNION ALL
    SELECT
        'undelegate' AS action,
        tx_id,
        msg_group,
        "'sender'" :: STRING AS delegator_address,
        "'amount'" AS amount,
        "'validator'" AS validator_address,
        NULL AS redelegate_source_validator_address,
        "'completion_time'" AS completion_time
    FROM
        (
            SELECT
                A.tx_ID,
                A.attribute_value,
                A.attribute_key,
                msg_group
            FROM
                msg_attr A
                LEFT JOIN (
                    SELECT
                        tx_ID,
                        msg_index
                    FROM
                        msg_attr
                    WHERE
                        event_Type = 'undelegate'
                ) b
                ON A.tx_ID = b.tx_ID
                AND A.msg_index = b.msg_index
                LEFT JOIN (
                    SELECT
                        tx_ID,
                        msg_index + 1 AS msg_index
                    FROM
                        msg_attr
                    WHERE
                        event_Type = 'undelegate'
                ) C
                ON A.tx_ID = C.tx_ID
                AND A.msg_index = C.msg_index
            WHERE
                (
                    b.tx_ID IS NOT NULL
                    OR C.tx_ID IS NOT NULL
                )
                AND event_Type = 'undelegate'
                AND attribute_key IN (
                    'sender',
                    'validator',
                    'amount',
                    'completion_time'
                )
        ) x pivot(MAX(attribute_value) for attribute_key IN ('amount', 'validator', 'sender', 'completion_time')) AS p
    UNION ALL
    SELECT
        'withdraw_rewards' AS action,
        tx_ID,
        msg_group,
        "'sender'" :: STRING AS delegator_address,
        "'amount'" AS amount,
        "'validator'" AS validator_address,
        NULL AS redelegate_source_validator_address,
        NULL AS completion_time
    FROM
        (
            SELECT
                A.tx_ID,
                A.attribute_value,
                A.attribute_key,
                msg_group
            FROM
                msg_attr A
                LEFT JOIN (
                    SELECT
                        tx_ID,
                        msg_index
                    FROM
                        msg_attr
                    WHERE
                        event_Type = 'withdraw_rewards'
                ) b
                ON A.tx_ID = b.tx_ID
                AND A.msg_index = b.msg_index
                LEFT JOIN (
                    SELECT
                        tx_ID,
                        msg_index + 1 AS msg_index
                    FROM
                        msg_attr
                    WHERE
                        event_Type = 'withdraw_rewards'
                ) C
                ON A.tx_ID = C.tx_ID
                AND A.msg_index = C.msg_index
            WHERE
                (
                    b.tx_ID IS NOT NULL
                    OR C.tx_ID IS NOT NULL
                )
                AND event_Type = 'withdraw_rewards'
                AND attribute_key IN (
                    'sender',
                    'validator',
                    'amount'
                )
        ) x pivot(MAX(attribute_value) for attribute_key IN ('amount', 'validator', 'sender')) AS p -- UNION ALL
        -- SELECT
        --     'superfluid delegate' AS action,
        --     tx_ID,
        --     msg_group,
        --     "'sender'" :: STRING AS delegator_address,
        --     "'amount'" AS amount,
        --     NULL AS validator_address,
        --     NULL AS redelegate_source_validator_address,
        --     NULL AS completion_time
        -- FROM
        --     (
        --         SELECT
        --             A.tx_ID,
        --             A.attribute_value,
        --             A.attribute_key,
        --             msg_group
        --         FROM
        --             msg_attr A
        --             LEFT JOIN (
        --                 SELECT
        --                     tx_ID,
        --                     msg_index
        --                 FROM
        --                     msg_attr
        --                 WHERE
        --                     event_Type = 'lock_and_superfluid_delegate'
        --             ) b
        --             ON A.tx_ID = b.tx_ID
        --             AND A.msg_index = b.msg_index
        --             LEFT JOIN (
        --                 SELECT
        --                     tx_ID,
        --                     msg_index + 1 AS msg_index
        --                 FROM
        --                     msg_attr
        --                 WHERE
        --                     event_Type = 'lock_and_superfluid_delegate'
        --             ) C
        --             ON A.tx_ID = C.tx_ID
        --             AND A.msg_index = C.msg_index
        --         WHERE
        --             (
        --                 b.tx_ID IS NOT NULL
        --                 OR C.tx_ID IS NOT NULL
        --             )
        --             AND event_Type = 'lock_and_superfluid_delegate'
        --             AND attribute_key IN (
        --                 'sender',
        --                 'validator',
        --                 'amount'
        --             )
        --     ) x pivot(MAX(attribute_value) for attribute_key IN ('amount', 'validator', 'sender')) AS p
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
