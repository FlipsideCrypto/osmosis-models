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
message_index_ibc AS (
    SELECT
        att.tx_id,
        MAX(
            att.msg_index
        ) AS max_index
    FROM
        {{ ref('silver__msg_attributes') }}
        att
        INNER JOIN sender s
        ON att.tx_id = s.tx_id
    WHERE
        msg_type = 'coin_spent'
        OR msg_type = 'transfer'
        AND attribute_key = 'amount'
        AND att.msg_index > s.msg_index

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
    att.tx_id
),
coin_sent_ibc AS (
    SELECT
        A.tx_id,
        COALESCE(
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
            ),
            TRY_PARSE_JSON(attribute_value) :amount
        ) AS amount,
        COALESCE(
            RIGHT(attribute_value, LENGTH(attribute_value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))),
            TRY_PARSE_JSON(attribute_value) [1] :denom
        ) AS currency,
        l.raw_metadata [1] :exponent AS DECIMAL
    FROM
        {{ ref('silver__msg_attributes') }} A
        LEFT OUTER JOIN message_index_ibc m
        ON A.tx_id = m.tx_id
        LEFT OUTER JOIN {{ ref('silver__asset_metadata') }}
        l
        ON RIGHT(attribute_value, LENGTH(attribute_value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))) = l.address
    WHERE
        A.msg_index = m.max_index
        AND A.attribute_key = 'amount'

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
receiver_ibc AS (
    SELECT
        tx_id,
        COALESCE(
            attribute_value,
            TRY_PARSE_JSON(attribute_value) :receiver
        ) AS receiver,
        MAX(msg_index) AS msg_index
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        msg_type = 'ibc_transfer'
        AND attribute_key = 'receiver'

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
    tx_id,
    receiver
),
osmo_tx_ids AS (
    SELECT
        DISTINCT tx_id
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        (
            msg_type = 'message'
            AND attribute_key = 'module'
            AND attribute_value = 'bank'
        )
        OR msg_type = 'claim'

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
message_indexes_osmo AS (
    SELECT
        v.tx_id,
        attribute_key,
        m.msg_index
    FROM
        osmo_tx_ids v
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
osmo_receiver AS (
    SELECT
        o.tx_id,
        m.msg_index,
        attribute_value AS receiver
    FROM
        osmo_tx_ids o
        LEFT OUTER JOIN {{ ref('silver__msg_attributes') }}
        m
        ON o.tx_id = m.tx_id
        LEFT OUTER JOIN message_indexes_osmo idx
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
osmo_amount AS (
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
        osmo_tx_ids o
        LEFT OUTER JOIN {{ ref('silver__msg_attributes') }}
        m
        ON o.tx_id = m.tx_id
        LEFT OUTER JOIN message_indexes_osmo idx
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
    r.tx_id,
    t.tx_succeeded,
    'IBC_TRANSFER_OUT' AS transfer_type,
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
    ) AS _unique_key
FROM
    receiver_ibc r
    LEFT OUTER JOIN coin_sent_ibc C
    ON r.tx_id = C.tx_id
    LEFT OUTER JOIN sender s
    ON r.tx_id = s.tx_id
    LEFT OUTER JOIN {{ ref('silver__transactions') }}
    t
    ON r.tx_id = t.tx_id
WHERE
    (
        amount IS NOT NULL
        OR currency IS NOT NULL
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
UNION ALL
SELECT
    block_id,
    block_timestamp,
    r.tx_id,
    t.tx_succeeded,
    'OSMOSIS' AS transfer_type,
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
    ) AS _unique_key
FROM
    osmo_receiver r
    LEFT OUTER JOIN osmo_amount C
    ON r.tx_id = C.tx_id
    AND r.msg_index = C.msg_index
    LEFT OUTER JOIN sender s
    ON r.tx_id = s.tx_id
    LEFT OUTER JOIN {{ ref('silver__transactions') }}
    t
    ON r.tx_id = t.tx_id
WHERE
    (
        amount IS NOT NULL
        OR currency IS NOT NULL
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
UNION ALL
SELECT
    m.block_id,
    m.block_timestamp,
    s.tx_id,
    m.tx_succeeded,
    'IBC_TRANSFER_IN' AS transfer_type,
    m.msg_index,
    TRY_PARSE_JSON(attribute_value) :sender :: STRING AS sender,
    C.amount :: NUMBER AS amount,
    C.currency,
    raw_metadata [1] :exponent :: INTEGER AS DECIMAL,
    TRY_PARSE_JSON(attribute_value) :receiver :: STRING AS receiver,
    m._inserted_timestamp,
    concat_ws(
        '-',
        s.tx_id,
        m.msg_index,
        currency
    ) AS _unique_key
FROM
    sender s
    LEFT OUTER JOIN {{ ref('silver__msg_attributes') }}
    m
    ON s.tx_id = m.tx_id
    LEFT OUTER JOIN {{ ref('silver__asset_metadata') }} A
    ON TRY_PARSE_JSON(attribute_value) :denom :: STRING = COALESCE(
        raw_metadata [0] :aliases [0] :: STRING,
        raw_metadata [0] :denom :: STRING
    )
    LEFT OUTER JOIN {{ ref('silver__transactions') }}
    t
    ON s.tx_id = t.tx_id
    INNER JOIN coin_sent_ibc C
    ON s.tx_id = C.tx_id
WHERE
    TRY_PARSE_JSON(attribute_value) :sender :: STRING IS NOT NULL
    AND m.msg_type = 'write_acknowledgement'
    AND m.attribute_key = 'packet_data'
    AND (
        amount IS NOT NULL
        OR currency IS NOT NULL
    )

{% if is_incremental() %}
AND m._inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        max_date
)
AND t._inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        max_date
)
{% endif %}
