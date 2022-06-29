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

sender AS (
    SELECT
        tx_id,
        msg_index,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS sender
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        attribute_key = 'acc_seq'

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
tx_ids AS (
    SELECT
        DISTINCT tx_id
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        attribute_key = 'sender'
        AND attribute_value = 'osmo1m5dncvfv7lvpvycr23zja93fecun2kcv226glq'
        AND block_timestamp >= '2021-06-18'
        AND block_timestamp <= '2021-12-31'

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
message_indexes AS (
    SELECT
        v.tx_id,
        attribute_key,
        m.msg_index
    FROM
        tx_ids v
        LEFT OUTER JOIN {{ ref('silver__msg_attributes') }}
        m
        ON v.tx_id = m.tx_id
        INNER JOIN sender s
        ON v.tx_id = s.tx_id
    WHERE
        msg_type = 'transfer'
        AND attribute_key = 'amount'
        AND m.msg_index > s.msg_index

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
areceiver AS (
    SELECT
        o.tx_id,
        m.msg_index,
        attribute_value AS receiver
    FROM
        tx_ids o
        LEFT OUTER JOIN {{ ref('silver__msg_attributes') }}
        m
        ON o.tx_id = m.tx_id
        LEFT OUTER JOIN message_indexes idx
        ON idx.tx_id = m.tx_id
    WHERE
        m.msg_type = 'transfer'
        AND m.attribute_key = 'recipient'
        AND idx.msg_index = m.msg_index

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
aamount AS (
    SELECT
        o.tx_id,
        m.msg_index,
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
        ) AS amount,
        RIGHT(attribute_value, LENGTH(attribute_value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))) AS currency,
        l.raw_metadata [1] :exponent AS DECIMAL
    FROM
        tx_ids o
        LEFT OUTER JOIN {{ ref('silver__msg_attributes') }}
        m
        ON o.tx_id = m.tx_id
        LEFT OUTER JOIN message_indexes idx
        ON idx.tx_id = m.tx_id
        LEFT OUTER JOIN {{ ref('silver__asset_metadata') }}
        l
        ON RIGHT(attribute_value, LENGTH(attribute_value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))) = l.address
    WHERE
        m.msg_type = 'transfer'
        AND m.attribute_key = 'amount'
        AND idx.msg_index = m.msg_index

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
    blockchain,
    chain_id,
    r.tx_id,
    tx_status,
    'AIRDROP' AS transfer_type,
    r.msg_index,
    sender,
    amount,
    currency,
    DECIMAL,
    receiver,
    _inserted_timestamp,
    concat_ws(
        '-',
        r.tx_id,
        r.msg_index,
        currency
    ) _unique_key
FROM
    areceiver r
    LEFT OUTER JOIN aamount C
    ON r.tx_id = C.tx_id
    AND r.msg_index = C.msg_index
    LEFT OUTER JOIN sender s
    ON r.tx_id = s.tx_id
    LEFT OUTER JOIN {{ ref('silver__transactions') }}
    t
    ON r.tx_id = t.tx_id
WHERE
    currency IS NOT NULL
    AND amount IS NOT NULL

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
