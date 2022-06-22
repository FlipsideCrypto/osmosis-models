{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH in_play AS (

    SELECT
        tx_ID,
        msg_group,
        msg_sub_group
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        msg_type IN(
            'pool_exited',
            'pool_joined'
        )

{% if is_incremental() %}
AND _ingested_at >= (
    SELECT
        MAX(
            _ingested_at
        )
    FROM
        {{ this }}
)
{% endif %}
),
msg_atts AS (
    SELECT
        DISTINCT A.tx_id,
        CASE
            WHEN attribute_key IN (
                'tokens_in',
                'tokens_out',
                'amount',
                'sender'
            ) THEN msg_index
        END msg_index,
        A.msg_group,
        A.msg_sub_group,
        msg_type,
        attribute_key,
        attribute_value,
        CASE
            WHEN msg_type = 'transfer'
            AND attribute_key = 'amount'
            AND attribute_value LIKE '%gamm%pool%'
            AND attribute_value NOT LIKE '%,%' THEN 'lp tokens'
            WHEN attribute_key = 'pool_id' THEN 'pool'
            WHEN msg_type IN(
                'pool_exited',
                'pool_joined'
            ) THEN 'non lp tokens'
            WHEN attribute_key = 'acc_seq' THEN 'lper'
            WHEN attribute_key = 'sender' THEN 'msg sender'
        END what_is_this,
        block_timestamp
    FROM
        {{ ref('silver__msg_attributes') }} A
        JOIN in_play b
        ON A.tx_id = b.tx_ID
    WHERE
        (
            (
                msg_type IN (
                    'pool_exited',
                    'pool_joined'
                )
            )
            AND (
                attribute_key IN (
                    'tokens_in',
                    'tokens_out',
                    'pool_id'
                )
            )
            OR attribute_key = 'acc_seq'
            OR (
                msg_type = 'transfer'
                AND attribute_key = 'amount'
                AND attribute_value LIKE '%gamm%pool%'
                AND attribute_value NOT LIKE '%,%'
            )
            OR (
                msg_type = 'message'
                AND attribute_key = 'sender'
            )
        )

{% if is_incremental() %}
AND _ingested_at >= (
    SELECT
        MAX(
            _ingested_at
        )
    FROM
        {{ this }}
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
        what_is_this,
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
        msg_group,
        msg_sub_group,
        what_is_this,
        amount,
        currency,
        CASE
            WHEN currency LIKE '%pool%' THEN 18
            ELSE raw_metadata [1] :exponent
        END AS DECIMAL,
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
        _ingested_at
    FROM
        {{ ref('silver__transactions') }}
),
act AS (
    SELECT
        tx_id,
        msg_group,
        msg_type AS action
    FROM
        msg_atts
    WHERE
        attribute_key IN(
            'tokens_in',
            'tokens_out'
        )
)
SELECT
    tx.block_id,
    tx.block_timestamp,
    tx.blockchain,
    tx.chain_id,
    d.tx_id,
    tx_status,
    d.msg_index,
    l.liquidity_provider_address,
    CASE
        WHEN act.action = 'pool_joined'
        AND what_is_this = 'lp tokens' THEN 'lp_tokens_minted'
        WHEN act.action = 'pool_exited'
        AND what_is_this = 'lp tokens' THEN 'lp_tokens_burned'
        WHEN act.action = 'pool_joined'
        AND what_is_this = 'non lp tokens' THEN 'pool_joined'
        WHEN act.action = 'pool_exited'
        AND what_is_this = 'non lp tokens' THEN 'pool_exited'
    END action,
    pool_id,
    amount,
    currency,
    DECIMAL,
    _ingested_at,
    concat_ws(
        '-',
        d.tx_id,
        d.msg_index,
        d.currency
    ) AS _unique_key
FROM
    decimals d
    JOIN act
    ON d.tx_id = act.tx_id
    AND d.msg_group = act.msg_group
    AND d.msg_group = act.msg_group
    JOIN pools p
    ON d.tx_id = p.tx_id
    AND d.msg_group = p.msg_group
    AND d.msg_sub_group = p.msg_sub_group
    AND d.msg_group = p.msg_group
    JOIN lper l
    ON d.tx_id = l.tx_id
    JOIN txn tx
    ON d.tx_id = tx.tx_id
    AND d.block_timestamp = tx.block_timestamp
