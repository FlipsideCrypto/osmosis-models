{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE'],
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

in_play AS (
    SELECT
        tx_ID,
        msg_group,
        msg_sub_group,
        attribute_value action,
        block_timestamp
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        msg_type = 'message'
        AND attribute_key = 'action'
        AND attribute_value IN (
            'join_pool',
            'exit_pool'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        max_date
)
{% endif %}
EXCEPT
SELECT
    tx_ID,
    msg_group,
    msg_sub_group,
    CASE
        msg_type
        WHEN 'pool_exited' THEN 'exit_pool'
        WHEN 'pool_joined' THEN 'join_pool'
    END action,
    block_timestamp
FROM
    {{ ref('silver__msg_attributes') }}
WHERE
    msg_type IN(
        'pool_exited',
        'pool_joined'
    )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        max_date
)
{% endif %}
),
msg_atts AS (
    SELECT
        CASE
            WHEN msg_type = 'message'
            AND attribute_key = 'action'
            AND attribute_value IN (
                'join_pool',
                'exit_pool'
            ) THEN 'action'
            WHEN msg_type = 'transfer'
            AND attribute_key = 'amount'
            AND attribute_value LIKE '%gamm%pool%'
            AND attribute_value NOT LIKE '%,%' THEN 'lp tokens'
            WHEN msg_type = 'transfer'
            AND attribute_key = 'amount' THEN 'non lp tokens'
            WHEN attribute_key = 'acc_seq' THEN 'lper'
            WHEN attribute_key = 'sender' THEN 'msg sender'
        END what_is_this,
        b.action,
        A.tx_ID,
        A.msg_group,
        A.msg_sub_group,
        A.msg_index,
        A.attribute_key,
        A.attribute_value,
        A.block_timestamp
    FROM
        {{ ref('silver__msg_attributes') }} A
        JOIN in_play b
        ON A.tx_id = b.tx_ID
    WHERE
        (
            (
                msg_type = 'message'
                AND attribute_key = 'action'
                AND attribute_value IN (
                    'join_pool',
                    'exit_pool'
                )
                AND A.msg_sub_group = b.msg_sub_group
                AND A.msg_group = b.msg_group
            )
            OR (
                A.msg_type = 'transfer'
                AND A.attribute_key = 'amount'
                AND A.msg_sub_group = b.msg_sub_group
                AND A.msg_group = b.msg_group
                AND (
                    attribute_value LIKE '%gamm%pool%'
                    OR attribute_value LIKE '%,%'
                )
            )
            OR attribute_key = 'acc_seq'
            OR (
                msg_type = 'message'
                AND attribute_key = 'sender'
            )
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        max_date
)
{% endif %}
),
lper AS (
    SELECT
        tx_id,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS liquidity_provider_address
    FROM
        msg_atts
    WHERE
        what_is_this = 'lper'
),
tokens AS (
    SELECT
        tx_id,
        msg_index,
        msg_group,
        msg_sub_group,
        SPLIT_PART(
            TRIM(
                REGEXP_REPLACE(
                    t.value,
                    '[^[:digit:]]',
                    ' '
                )
            ),
            ' ',
            0
        ) :: INTEGER AS amount,
        RIGHT(t.value, LENGTH(t.value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(t.value, '[^[:digit:]]', ' ')), ' ', 0))) :: STRING AS currency,
        what_is_this,
        action,
        block_timestamp
    FROM
        msg_atts,
        LATERAL SPLIT_TO_TABLE (
            attribute_value,
            ','
        ) t
    WHERE
        what_is_this IN (
            'lp tokens',
            'non lp tokens'
        )
),
sndr AS (
    SELECT
        tx_id,
        msg_index,
        attribute_value
    FROM
        msg_atts
    WHERE
        attribute_key = 'sender'
),
tokens_2 AS (
    SELECT
        A.tx_id,
        A.msg_index,
        A.msg_group,
        A.msg_sub_group,
        A.what_is_this,
        A.amount,
        A.currency,
        A.action,
        A.block_timestamp
    FROM
        tokens A
        LEFT JOIN lper b
        ON A.tx_id = b.tx_id
        LEFT JOIN sndr C
        ON A.tx_Id = C.tx_ID
        AND b.liquidity_provider_address = C.attribute_value
        AND A.msg_index = C.msg_index + 1
    WHERE
        what_is_this = 'non lp tokens'
        OR (
            what_is_this = 'lp tokens'
            AND C.tx_ID IS NOT NULL
        )
),
decimals AS (
    SELECT
        tx_id,
        msg_index,
        t.msg_group,
        t.msg_sub_group,
        what_is_this,
        amount,
        currency,
        CASE
            WHEN currency LIKE '%pool%' THEN 18
            ELSE raw_metadata [1] :exponent
        END AS DECIMAL,
        action,
        block_timestamp
    FROM
        tokens_2 t
        LEFT OUTER JOIN {{ ref('silver__asset_metadata') }}
        ON currency = address
),
pools AS (
    SELECT
        tx_id,
        msg_group,
        msg_sub_group,
        REPLACE(SPLIT_PART(currency, 'gamm', 2), '/pool/') :: INT pool_id
    FROM
        tokens_2
    WHERE
        what_is_this = 'lp tokens'
),
txn AS (
    SELECT
        block_id,
        block_timestamp,
        blockchain,
        chain_id,
        tx_id,
        tx_status,
        _inserted_timestamp
    FROM
        {{ ref('silver__transactions') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            max_date
    )
{% endif %}
)
SELECT
    tx.block_id,
    tx.block_timestamp,
    tx.blockchain,
    tx.chain_id,
    tx.tx_id,
    tx.tx_status,
    d.msg_index,
    l.liquidity_provider_address,
    CASE
        WHEN action = 'join_pool'
        AND what_is_this = 'lp tokens' THEN 'lp_tokens_minted'
        WHEN action = 'exit_pool'
        AND what_is_this = 'lp tokens' THEN 'lp_tokens_burned'
        WHEN action = 'join_pool'
        AND what_is_this = 'non lp tokens' THEN 'pool_joined'
        WHEN action = 'exit_pool'
        AND what_is_this = 'non lp tokens' THEN 'pool_exited'
    END action,
    p.pool_id :: ARRAY AS pool_id,
    d.amount,
    d.currency,
    d.decimal,
    tx._inserted_timestamp,
    concat_ws(
        '-',
        d.tx_id,
        msg_index,
        currency
    ) AS _unique_key
FROM
    decimals d
    JOIN pools p
    ON d.tx_id = p.tx_id
    AND d.msg_group = p.msg_group
    AND d.msg_sub_group = p.msg_sub_group
    JOIN lper l
    ON d.tx_id = l.tx_id
    JOIN txn tx
    ON d.tx_id = tx.tx_id
    AND d.block_timestamp = tx.block_timestamp
