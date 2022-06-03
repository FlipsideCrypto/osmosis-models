{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, msg_index)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH message_indexes AS (

    SELECT
        tx_id,
        attribute_key,
        msg_index
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        (
            msg_type = 'pool_exited'
            OR msg_type = 'pool_joined'
        )
        AND (
            attribute_key = 'tokens_in'
            OR attribute_key = 'tokens_out'
        )

{% if is_incremental() %}
AND _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
),
pool_ids AS (
    SELECT
        A.tx_id,
        A.msg_index,
        attribute_value :: INTEGER AS pool_id
    FROM
        {{ ref('silver__msg_attributes') }} A
        LEFT OUTER JOIN message_indexes m
        ON A.tx_id = m.tx_id
    WHERE
        (
            msg_type = 'pool_exited'
            OR msg_type = 'pool_joined'
        )
        AND A.attribute_key = 'pool_id'
        AND A.msg_index = m.msg_index

{% if is_incremental() %}
AND _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
),
token_array AS (
    SELECT
        A.tx_id,
        A.msg_index,
        msg_type AS action,
        SPLIT(
            attribute_value,
            ','
        ) AS tokens
    FROM
        {{ ref('silver__msg_attributes') }} A
        LEFT OUTER JOIN message_indexes m
        ON A.tx_id = m.tx_id
        AND A.attribute_key = m.attribute_key
    WHERE
        (
            msg_type = 'pool_exited'
            OR msg_type = 'pool_joined'
        )
        AND (
            A.attribute_key = 'tokens_in'
            OR A.attribute_key = 'tokens_out'
        )
        AND A.msg_index = m.msg_index

{% if is_incremental() %}
AND _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
),
tokens AS (
    SELECT
        tx_id,
        msg_index,
        action,
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
        RIGHT(t.value, LENGTH(t.value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(t.value, '[^[:digit:]]', ' ')), ' ', 0))) :: STRING AS currency
    FROM
        token_array,
        LATERAL FLATTEN (
            input => tokens
        ) t
),
decimals AS (
    SELECT
        tx_id,
        msg_index,
        action,
        amount,
        currency,
        raw_metadata [1] :exponent AS DECIMAL
    FROM
        tokens t
        LEFT OUTER JOIN {{ ref('silver__asset_metadata') }}
        ON currency = address
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
        {{ ref('silver__msg_attributes') }}
    WHERE
        attribute_key = 'acc_seq'

{% if is_incremental() %}
AND _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
)
SELECT
    tx.block_id,
    tx.block_timestamp,
    tx.blockchain,
    tx.chain_id,
    d.tx_id,
    tx_status,
    d.msg_index,
    liquidity_provider_address,
    action,
    pool_id,
    amount,
    currency,
    DECIMAL,
    _ingested_at
FROM
    decimals d
    LEFT OUTER JOIN pool_ids p
    ON d.tx_id = p.tx_id
    AND d.msg_index = p.msg_index
    LEFT OUTER JOIN lper l
    ON d.tx_id = l.tx_id
    LEFT OUTER JOIN {{ ref('silver__transactions') }}
    tx
    ON d.tx_id = tx.tx_id

{% if is_incremental() %}
AND _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
