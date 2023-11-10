{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['noncore']
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
        msg_sub_group
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        msg_type = 'message'
        AND attribute_key = 'action'
        AND attribute_value IN (
            'unpool_whitelisted_pool',
            '/osmosis.superfluid.MsgUnPoolWhitelistedPool'
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
msg_atts_raw AS (
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
        COALESCE(
            A.msg_sub_group,
            -1
        ) AS msg_sub_group,
        msg_type,
        attribute_key,
        attribute_value,
        CASE
            WHEN attribute_key = 'module' THEN 'module'
            WHEN msg_type IN(
                'pool_exited',
                'pool_joined'
            )
            AND attribute_key <> 'pool_id' THEN 'non lp tokens'
            WHEN msg_type = 'burn' THEN 'lp tokens'
            WHEN attribute_key = 'pool_id' THEN 'pool'
            WHEN attribute_key = 'acc_seq' THEN 'lper'
            WHEN attribute_key = 'sender' THEN 'msg sender'
        END what_is_this,
        block_timestamp,
        CASE
            WHEN what_is_this = 'lp tokens' THEN TRUE
            ELSE FALSE
        END AS is_module
    FROM
        {{ ref('silver__msg_attributes') }} A
        JOIN in_play b
        ON A.tx_id = b.tx_ID
    WHERE
        (
            (
                msg_type IN (
                    'pool_exited',
                    'pool_joined',
                    'burn'
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
                msg_type = 'burn'
                AND attribute_key = 'amount'
            )
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
msg_atts AS (
    SELECT
        tx_id,
        msg_index,
        NULLIF(
            (conditional_true_event(is_module) over (PARTITION BY tx_id
            ORDER BY
                msg_index) -1),
                -1
        ) AS msg_group,
        msg_sub_group,
        what_is_this,
        msg_type,
        attribute_key,
        attribute_value,
        block_timestamp,
        is_module
    FROM
        msg_atts_raw
),
lper AS (
    SELECT
        DISTINCT tx_id,
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
        ) qualify(ROW_NUMBER() over(PARTITION BY tx_id, msg_group, msg_sub_group, currency
    ORDER BY
        amount DESC) = 1)
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
        LEFT JOIN (
            SELECT
                DISTINCT tx_id,
                attribute_value
            FROM
                sndr
        ) C
        ON A.tx_Id = C.tx_ID
        AND b.liquidity_provider_address = C.attribute_value
    WHERE
        what_is_this = 'non lp tokens'
        OR (
            what_is_this = 'lp tokens'
            AND (
                C.tx_ID IS NOT NULL
            )
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
            ELSE DECIMAL
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
        tx_id,
        tx_succeeded,
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
),
act AS (
    SELECT
        tx_id,
        msg_group,
        msg_sub_group,
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
    d.tx_id,
    tx_succeeded,
    d.msg_index,
    d.msg_group,
    d.msg_sub_group,
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
    _inserted_timestamp,
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
    AND d.msg_sub_group = act.msg_sub_group
    JOIN pools p
    ON d.tx_id = p.tx_id
    AND d.msg_group = p.msg_group
    AND d.msg_sub_group = p.msg_sub_group
    JOIN lper l
    ON d.tx_id = l.tx_id
    JOIN txn tx
    ON d.tx_id = tx.tx_id
    AND d.block_timestamp = tx.block_timestamp
