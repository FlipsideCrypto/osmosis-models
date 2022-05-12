{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['_ingested_at::DATE'],
) }}

WITH message_indexes AS (

    SELECT
        tx_id,
        attribute_key,
        MIN(msg_index) AS min_index,
        MAX(msg_index) AS max_index
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        msg_type = 'token_swapped'
        AND (
            attribute_key = 'tokens_in'
            OR attribute_key = 'tokens_out'
        )

{% if is_incremental() %}
AND _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
GROUP BY
    tx_id,
    attribute_key
),
tokens_in AS (
    SELECT
        t.tx_id,
        SPLIT_PART(
            TRIM(
                REGEXP_REPLACE(
                    attribute_value,
                    '[^[:digit:]]',
                    ' '
                )
            ),
            ' ',
            0
        ) AS swap_from_amount,
        RIGHT(attribute_value, LENGTH(attribute_value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))) AS swap_from_currency,
        l.raw_metadata [1] :exponent AS swap_from_decimal
    FROM
        {{ ref('silver__msg_attributes') }}
        t
        LEFT OUTER JOIN message_indexes m
        ON t.tx_id = m.tx_id
        AND t.attribute_key = m.attribute_key
        LEFT OUTER JOIN {{ ref('silver__asset_metadata') }}
        l
        ON RIGHT(attribute_value, LENGTH(attribute_value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))) = l.address
    WHERE
        msg_type = 'token_swapped'
        AND t.attribute_key = 'tokens_in'
        AND t.msg_index = m.min_index

{% if is_incremental() %}
AND _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
),
tokens_out AS (
    SELECT
        t.tx_id,
        SPLIT_PART(
            TRIM(
                REGEXP_REPLACE(
                    attribute_value,
                    '[^[:digit:]]',
                    ' '
                )
            ),
            ' ',
            0
        ) AS swap_to_amount,
        RIGHT(attribute_value, LENGTH(attribute_value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))) AS swap_to_currency,
        l.raw_metadata [1] :exponent AS swap_to_decimal
    FROM
        {{ ref('silver__msg_attributes') }}
        t
        LEFT OUTER JOIN message_indexes m
        ON t.tx_id = m.tx_id
        AND t.attribute_key = m.attribute_key
        LEFT OUTER JOIN {{ ref('silver__asset_metadata') }}
        l
        ON RIGHT(attribute_value, LENGTH(attribute_value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))) = l.address
    WHERE
        msg_type = 'token_swapped'
        AND t.attribute_key = 'tokens_out'
        AND t.msg_index = m.max_index

{% if is_incremental() %}
AND _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
),
pools AS (
    SELECT
        tx_id,
        ARRAY_AGG(
            attribute_value :: INTEGER
        ) AS pool_ids
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        attribute_key = 'pool_id'

{% if is_incremental() %}
AND _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
GROUP BY
    tx_id
),
trader AS (
    SELECT
        tx_id,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS trader
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        attribute_key = 'acc_seq'

{% if is_incremental() %}
AND _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
)
SELECT
    t.block_id,
    t.block_timestamp,
    t.blockchain,
    t.chain_id,
    t.tx_id,
    t.tx_status,
    s.trader,
    f.swap_from_amount AS swap_from_amount,
    f.swap_from_currency,
    CASE
        WHEN f.swap_from_currency LIKE 'gamm/pool/%' THEN 18
        ELSE f.swap_from_decimal
    END AS swap_from_decimal,
    tt.swap_to_amount,
    tt.swap_to_currency,
    CASE
        WHEN tt.swap_to_currency LIKE 'gamm/pool/%' THEN 18
        ELSE tt.swap_to_decimal
    END AS swap_to_decimal,
    pool_ids,
    t._ingested_at
FROM
    tokens_in f
    LEFT OUTER JOIN {{ ref('silver__transactions') }}
    t
    ON f.tx_id = t.tx_id
    INNER JOIN tokens_out tt
    ON f.tx_id = tt.tx_id
    INNER JOIN trader s
    ON t.tx_id = s.tx_id
    INNER JOIN pools p
    ON t.tx_id = p.tx_id

{% if is_incremental() %}
WHERE
    t._ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
