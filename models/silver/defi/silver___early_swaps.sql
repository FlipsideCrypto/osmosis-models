{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
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

early_swaps AS (
    SELECT
        DISTINCT tx_id
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        block_timestamp :: DATE < '2021-09-25'
        AND msg_type = 'message'
        AND attribute_key = 'action'
        AND attribute_value = 'swap_exact_amount_in'

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
INDEXES AS (
    SELECT
        e.tx_id,
        MIN(
            m.msg_index
        ) AS index_in,
        MAX(
            m.msg_index
        ) AS index_out
    FROM
        early_swaps e
        LEFT OUTER JOIN {{ ref('silver__msg_attributes') }}
        m
        ON e.tx_id = m.tx_id
    WHERE
        m.msg_type = 'transfer'

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
GROUP BY
    e.tx_id
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
        block_timestamp :: DATE < '2021-09-25'
        AND attribute_key = 'acc_seq'

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
from_token AS (
    SELECT
        i.tx_id,
        RIGHT(attribute_value, LENGTH(attribute_value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))) AS from_currency,
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
        ) AS from_amount,
        A.raw_metadata [1] :exponent AS from_decimal
    FROM
        INDEXES i
        LEFT OUTER JOIN {{ ref('silver__msg_attributes') }}
        m
        ON i.tx_id = m.tx_id
        AND i.index_in = m.msg_index
        LEFT OUTER JOIN {{ ref('silver__asset_metadata') }} A
        ON RIGHT(attribute_value, LENGTH(attribute_value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))) = A.address
    WHERE
        msg_type = 'transfer'
        AND attribute_key = 'amount'

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
to_token AS (
    SELECT
        i.tx_id,
        RIGHT(attribute_value, LENGTH(attribute_value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))) AS to_currency,
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
        ) AS to_amount,
        A.raw_metadata [1] :exponent AS TO_DECIMAL
    FROM
        INDEXES i
        LEFT OUTER JOIN {{ ref('silver__msg_attributes') }}
        m
        ON i.tx_id = m.tx_id
        AND i.index_out = m.msg_index
        LEFT OUTER JOIN {{ ref('silver__asset_metadata') }} A
        ON RIGHT(attribute_value, LENGTH(attribute_value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))) = A.address
    WHERE
        msg_type = 'transfer'
        AND attribute_key = 'amount'

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
)
SELECT
    block_id,
    block_timestamp,
    {# blockchain, #}
    NULL AS chain_id,
    e.tx_id,
    NULL AS tx_status,
    tt.trader,
    from_amount :: NUMBER AS from_amount,
    from_currency,
    CASE
        WHEN f.from_currency LIKE 'gamm/pool/%' THEN 18
        ELSE f.from_decimal :: NUMBER
    END AS from_decimal,
    to_amount :: NUMBER AS to_amount,
    to_currency,
    CASE
        WHEN tok.to_currency LIKE 'gamm/pool/%' THEN 18
        ELSE tok.to_decimal :: NUMBER
    END AS TO_DECIMAL,
    NULL :: ARRAY AS pool_ids,
    _inserted_timestamp
FROM
    early_swaps e
    LEFT OUTER JOIN {{ ref('silver__transactions') }}
    t
    ON e.tx_id = t.tx_id
    INNER JOIN trader tt
    ON e.tx_id = tt.tx_id
    INNER JOIN from_token f
    ON e.tx_id = f.tx_id
    INNER JOIN to_token tok
    ON e.tx_id = tok.tx_id

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
