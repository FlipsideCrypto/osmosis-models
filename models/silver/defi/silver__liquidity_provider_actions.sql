{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE']
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
        DISTINCT tx_id,
        msg_group,
        COALESCE(
            msg_sub_group,
            -1
        ) AS msg_sub_group
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        block_timestamp :: DATE >= '2021-09-24'
        AND msg_type IN(
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
EXCEPT
SELECT
    tx_id,
    msg_group,
    COALESCE(
        msg_sub_group,
        -1
    ) AS msg_sub_group
FROM
    {{ ref('silver__msg_attributes') }}
WHERE
    block_timestamp :: DATE >= '2021-09-24'
    AND msg_type ILIKE '%apollo%'

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
        A.block_id,
        A.block_timestamp,
        A.tx_id,
        A.tx_succeeded,
        A._inserted_timestamp,
        msg_index,
        A.msg_group,
        COALESCE(
            A.msg_sub_group,
            -1
        ) AS msg_sub_group,
        msg_type,
        attribute_key,
        attribute_value
    FROM
        {{ ref('silver__msg_attributes') }} A
        JOIN in_play b
        ON A.tx_id = b.tx_id
        AND A.msg_group = b.msg_group
        AND COALESCE(
            A.msg_sub_group,
            -1
        ) = b.msg_sub_group
    WHERE
        A.block_timestamp :: DATE >= '2021-09-24'
        AND (
            msg_type IN (
                'pool_exited',
                'pool_joined',
                'message'
            )
            OR (
                A.msg_type = 'transfer'
                AND A.msg_group IS NOT NULL
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
action AS (
    SELECT
        tx_id,
        msg_group,
        msg_sub_group,
        msg_type,
        attribute_value action
    FROM
        msg_atts
    WHERE
        msg_type = 'message'
        AND attribute_key = 'action'
),
lper AS (
    SELECT
        tx_id,
        msg_group,
        msg_sub_group,
        msg_type,
        attribute_value lper
    FROM
        msg_atts
    WHERE
        msg_type IN (
            'pool_exited',
            'pool_joined'
        )
        AND attribute_key = 'sender'
),
pool_tokens AS (
    SELECT
        tx_id,
        msg_group,
        msg_sub_group,
        msg_type,
        msg_index,
        attribute_value amount
    FROM
        msg_atts
    WHERE
        msg_type IN (
            'pool_exited',
            'pool_joined'
        )
        AND attribute_key LIKE 'token%'
),
rel_xfer_msg_index AS (
    SELECT
        A.tx_id,
        A.msg_group,
        A.msg_sub_group,
        A.msg_index
    FROM
        msg_atts A
        JOIN lper b
        ON A.tx_id = b.tx_id
        AND A.msg_group = b.msg_group
        AND A.msg_sub_group = b.msg_sub_group
        JOIN pool_tokens C
        ON A.tx_id = C.tx_id
        AND A.msg_group = C.msg_group
        AND A.msg_sub_group = C.msg_sub_group
        AND A.msg_index < C.msg_index
    WHERE
        A.msg_type = 'transfer'
        AND (
            (
                b.msg_type = 'pool_exited'
                AND A.attribute_key = 'sender'
                AND A.attribute_value = b.lper
            )
            OR (
                b.msg_type = 'pool_joined'
                AND A.attribute_key = 'sender'
                AND A.attribute_value <> b.lper
            )
        )
),
rel_xfer AS (
    SELECT
        A.tx_id,
        A.msg_group,
        A.msg_sub_group,
        A.msg_index,
        A.attribute_value AS amount
    FROM
        msg_atts A
        JOIN rel_xfer_msg_index b
        ON A.tx_id = b.tx_id
        AND A.msg_index = b.msg_index
    WHERE
        attribute_key = 'amount'
),
pool_id AS (
    SELECT
        DISTINCT tx_id,
        msg_group,
        msg_sub_group,
        attribute_value pool_id
    FROM
        msg_atts
    WHERE
        attribute_key = 'pool_id'
),
tokens AS (
    SELECT
        tx_id,
        msg_group,
        msg_sub_group,
        msg_type,
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
        (
            SELECT
                tx_id,
                msg_group,
                msg_sub_group,
                msg_type,
                amount
            FROM
                pool_tokens
            UNION ALL
            SELECT
                tx_id,
                msg_group,
                msg_sub_group,
                'xfer',
                amount AS msg_type
            FROM
                rel_xfer
        ),
        LATERAL SPLIT_TO_TABLE (
            amount,
            ','
        ) t
),
decimals AS (
    SELECT
        tx_id,
        msg_group,
        msg_sub_group,
        msg_type,
        SUM(amount) AS amount,
        currency,
        CASE
            WHEN currency LIKE '%pool%' THEN 18
            ELSE DECIMAL
        END AS DECIMAL
    FROM
        tokens t
        LEFT OUTER JOIN {{ ref('silver__asset_metadata') }}
        ON currency = address
    GROUP BY
        tx_id,
        msg_group,
        msg_sub_group,
        msg_type,
        currency,
        DECIMAL
),
txn AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        _inserted_timestamp
    FROM
        msg_atts qualify (ROW_NUMBER() over(PARTITION BY tx_id
    ORDER BY
        _inserted_timestamp DESC) = 1)
)
SELECT
    tx.block_id,
    tx.block_timestamp,
    d.tx_id,
    tx_succeeded,
    d.msg_group,
    d.msg_sub_group,
    l.lper AS liquidity_provider_address,
    CASE
        WHEN l.msg_type = 'pool_joined'
        AND d.msg_type = 'xfer' THEN 'lp_tokens_minted'
        WHEN l.msg_type = 'pool_exited'
        AND d.msg_type = 'xfer' THEN 'lp_tokens_burned'
        WHEN l.msg_type = 'pool_joined'
        AND d.msg_type = 'pool_joined' THEN 'pool_joined'
        WHEN l.msg_type = 'pool_exited'
        AND d.msg_type = 'pool_exited' THEN 'pool_exited'
    END action,
    p1.pool_id :: INT pool_id,
    amount,
    currency,
    DECIMAL,
    _inserted_timestamp,
    concat_ws(
        '-',
        d.tx_id,
        d.msg_group,
        d.msg_sub_group,
        d.currency
    ) AS _unique_key
FROM
    decimals d
    JOIN lper l
    ON d.tx_id = l.tx_id
    AND d.msg_group = l.msg_group
    AND d.msg_sub_group = l.msg_sub_group
    JOIN txn tx
    ON d.tx_id = tx.tx_id
    LEFT JOIN pool_id p1
    ON d.tx_id = p1.tx_id
    AND d.msg_group = p1.msg_group
    AND d.msg_sub_group = p1.msg_sub_group
    LEFT JOIN action act
    ON d.tx_id = act.tx_id
    AND d.msg_group = act.msg_group
    AND d.msg_sub_group = act.msg_sub_group
WHERE
    COALESCE(
        act.action,
        ''
    ) NOT IN (
        'unpool_whitelisted_pool',
        '/osmosis.superfluid.MsgUnPoolWhitelistedPool'
    )
