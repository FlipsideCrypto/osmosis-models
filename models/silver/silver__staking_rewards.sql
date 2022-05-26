{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, msg_group, action, currency, delegator_address, validator_address)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['_ingested_at::DATE'],
) }}

WITH msg_attributes_cte AS (

    SELECT
        tx_id,
        msg_type,
        msg_group,
        msg_index,
        attribute_key,
        attribute_value
    FROM
        {{ ref('silver__msg_attributes') }} A
    WHERE
        msg_type IN (
            'withdraw_rewards',
            'claim',
            'transfer',
            'message',
            'tx',
            'delegate',
            'redelegate',
            'unbond'
        )

{% if is_incremental() %}
AND _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
),
reward_base AS (
    SELECT
        A.tx_id,
        A.msg_type,
        A.msg_index,
        msg_group
    FROM
        msg_attributes_cte A
    WHERE
        msg_type = 'withdraw_rewards'
),
msg_attr_rewards AS (
    SELECT
        A.tx_id,
        A.msg_group,
        A.msg_index,
        A.msg_type,
        A.attribute_key,
        A.attribute_value,
        b.group_id
    FROM
        msg_attributes_cte A
        JOIN (
            SELECT
                DISTINCT tx_id,
                msg_index,
                msg_index group_id
            FROM
                reward_base
            UNION ALL
            SELECT
                DISTINCT tx_id,
                msg_index + 1 msg_index,
                msg_index group_id
            FROM
                reward_base
        ) b
        ON A.tx_ID = b.tx_ID
        AND A.msg_index = b.msg_index
),
reward_combo AS (
    SELECT
        tx_id,
        msg_group,
        group_id,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :validator :: STRING AS validator_address,
        j :amount :: STRING AS amount,
        j :sender :: STRING AS delegator_address
    FROM
        msg_attr_rewards
    WHERE
        attribute_key IN (
            'sender',
            'amount',
            'validator'
        )
    GROUP BY
        tx_id,
        msg_group,
        group_id
),
claim_base AS (
    SELECT
        A.tx_id,
        A.msg_type,
        A.msg_index,
        msg_group
    FROM
        msg_attributes_cte A
        INNER JOIN (
            SELECT
                DISTINCT tx_id
            FROM
                msg_attributes_cte
            WHERE
                msg_type IN (
                    'delegate',
                    'redelegate'
                )
        ) b
        ON A.tx_id = b.tx_id
    WHERE
        msg_type = 'claim'
),
msg_attr_claim AS (
    SELECT
        A.tx_id,
        A.attribute_key,
        A.attribute_value,
        A.msg_index,
        A.msg_type,
        A.msg_group,
        b.group_id
    FROM
        msg_attributes_cte A
        JOIN (
            SELECT
                DISTINCT tx_id,
                msg_index,
                msg_index group_id
            FROM
                claim_base
            UNION ALL
            SELECT
                DISTINCT tx_id,
                msg_index + 1 msg_index,
                msg_index group_id
            FROM
                claim_base
            UNION ALL
            SELECT
                DISTINCT tx_id,
                msg_index + 2 msg_index,
                msg_index group_id
            FROM
                claim_base
        ) b
        ON A.tx_ID = b.tx_ID
        AND A.msg_index = b.msg_index
),
claim_combo AS (
    SELECT
        tx_id,
        msg_group,
        group_id,
        OBJECT_AGG(
            CASE
                WHEN msg_type IN (
                    'message',
                    'claim'
                ) THEN msg_type || '__'
                ELSE ''
            END || attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        COALESCE(
            j :validator :: STRING,
            j :source_validator :: STRING
        ) AS validator_address,
        j :claim__amount :: STRING AS amount,
        j :message__sender :: STRING AS delegator_address
    FROM
        msg_attr_claim
    WHERE
        msg_type || '__' || attribute_key IN (
            'message__sender',
            'claim__amount'
        )
        OR attribute_key IN (
            'validator',
            'source_validator'
        )
    GROUP BY
        tx_id,
        msg_group,
        group_id
),
tran_base AS (
    SELECT
        DISTINCT tx_id,
        msg_group,
        delegator_address
    FROM
        {{ ref('silver__staking') }} A

{% if is_incremental() %}
WHERE
    _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
),
tran_tran AS (
    SELECT
        A.tx_id,
        A.msg_type,
        A.msg_index,
        A.msg_group,
        attribute_key,
        attribute_value
    FROM
        msg_attributes_cte A
        INNER JOIN tran_base b
        ON A.tx_id = b.tx_id
        AND A.msg_group = b.msg_group
        AND A.attribute_value = b.delegator_address
        LEFT JOIN (
            SELECT
                DISTINCT tx_id
            FROM
                msg_attributes_cte
            WHERE
                msg_type IN (
                    'claim',
                    'withdraw_rewards'
                )
        ) C
        ON A.tx_id = C.tx_id
    WHERE
        A.msg_type = 'transfer'
        AND A.attribute_key = 'recipient'
        AND C.tx_id IS NULL
),
msg_attr_trans AS (
    SELECT
        A.tx_id,
        A.msg_group,
        A.msg_index,
        A.msg_type,
        A.attribute_key,
        A.attribute_value,
        conditional_change_event(
            CASE
                WHEN A.msg_type IN (
                    'delegate',
                    'redelegate',
                    'unbond'
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
        tran_tran b
        JOIN msg_attributes_cte A
        ON A.tx_ID = b.tx_ID
        AND A.msg_group = b.msg_group
    WHERE
        A.msg_type IN (
            'delegate',
            'redelegate',
            'unbond'
        )
        OR (
            A.msg_index = b.msg_index
            AND A.msg_type = 'transfer'
        )
),
tran_combo AS (
    SELECT
        A.tx_id,
        A.msg_group,
        A.msg_index group_id,
        COALESCE(
            b.j :validator :: STRING,
            b.j :source_validator :: STRING
        ) AS validator_address,
        A.j :amount :: STRING AS amount,
        A.j :recipient :: STRING AS delegator_address
    FROM
        (
            SELECT
                tx_id,
                msg_group,
                msg_index,
                change_index + 1 group_id,
                OBJECT_AGG(
                    attribute_key :: STRING,
                    attribute_value :: variant
                ) AS j
            FROM
                (
                    SELECT
                        DISTINCT tx_id,
                        msg_group,
                        msg_index,
                        change_index,
                        attribute_key,
                        attribute_value
                    FROM
                        msg_attr_trans
                    WHERE
                        msg_type = 'transfer'
                        AND attribute_key IN (
                            'amount',
                            'recipient'
                        )
                ) x
            GROUP BY
                tx_id,
                msg_group,
                msg_index,
                group_id
        ) A
        JOIN (
            SELECT
                tx_id,
                msg_group,
                msg_index,
                change_index group_id,
                OBJECT_AGG(
                    attribute_key :: STRING,
                    attribute_value :: variant
                ) AS j
            FROM
                (
                    SELECT
                        DISTINCT tx_id,
                        msg_group,
                        msg_index,
                        change_index,
                        attribute_key,
                        attribute_value
                    FROM
                        msg_attr_trans
                    WHERE
                        msg_type <> 'transfer'
                        AND attribute_key IN (
                            'validator',
                            'source_validator'
                        )
                ) x
            GROUP BY
                tx_id,
                msg_group,
                msg_index,
                group_id
        ) b
        ON A.tx_id = b.tx_id
        AND A.msg_group = b.msg_group
        AND A.group_id = b.group_id
),
combo_all AS (
    SELECT
        tx_id,
        msg_group,
        group_id,
        validator_address,
        amount,
        delegator_address,
        'claim' AS action
    FROM
        tran_combo
    UNION ALL
    SELECT
        tx_id,
        msg_group,
        group_id,
        validator_address,
        amount,
        delegator_address,
        'withdraw_rewards' AS action
    FROM
        reward_combo
    UNION ALL
    SELECT
        tx_id,
        msg_group,
        group_id,
        validator_address,
        amount,
        delegator_address,
        'claim' AS action
    FROM
        claim_combo
),
tx_address AS (
    SELECT
        DISTINCT A.tx_id,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS tx_caller_address
    FROM
        msg_attributes_cte A
    WHERE
        attribute_key = 'acc_seq'
),
prefinal AS (
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
        ) AS amount,CASE
            WHEN A.split_amount LIKE '%uosmo' THEN 'uosmo'
            WHEN A.split_amount LIKE '%uion' THEN 'uion'
            WHEN A.split_amount LIKE '%pool%' THEN SUBSTRING(A.split_amount, CHARINDEX('g', A.split_amount), 99)
            WHEN A.split_amount LIKE '%ibc%' THEN SUBSTRING(A.split_amount, CHARINDEX('i', A.split_amount), 99)
            ELSE 'uosmo'
        END AS currency,
        A.validator_address,
        b._INGESTED_AT
    FROM
        (
            SELECT
                p.tx_Id,
                p.action,
                p.msg_group,
                p.delegator_address,
                p.validator_address,
                am.value AS split_amount
            FROM
                combo_all p,
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
                {{ ref('silver__transactions') }} A

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
    A.delegator_address,CASE
        WHEN A.split_amount LIKE '%uosmo' THEN 'uosmo'
        WHEN A.split_amount LIKE '%uion' THEN 'uion'
        WHEN A.split_amount LIKE '%pool%' THEN SUBSTRING(A.split_amount, CHARINDEX('g', A.split_amount), 99)
        WHEN A.split_amount LIKE '%ibc%' THEN SUBSTRING(A.split_amount, CHARINDEX('i', A.split_amount), 99)
        ELSE 'uosmo'
    END,
    A.validator_address,
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
    amd.raw_metadata [1] :exponent AS DECIMAL,
    A._INGESTED_AT
FROM
    prefinal A
    LEFT OUTER JOIN osmosis_dev.silver.asset_metadata amd
    ON A.currency = amd.address
