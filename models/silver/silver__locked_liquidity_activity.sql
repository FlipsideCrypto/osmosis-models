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

base_txn AS (
    SELECT
        block_ID,
        block_timestamp,
        blockchain,
        chain_id,
        tx_id,
        tx_status,
        msg_type AS actio,
        msg_index,
        msg :sender AS delegator_address,
        msg :coins [0] :amount AS amount,
        msg :coins [0] :denom AS currency,
        msg :val_addr AS validator_address,
        msg :duration,
        _inserted_timestamp,
        msg :lock_id AS lock_id
    FROM
        {{ ref('silver__tx_body_msgs') }}
    WHERE
        msg_type IN (
            '/osmosis.superfluid.MsgLockAndSuperfluidDelegate',
            '/osmosis.superfluid.MsgSuperfluidUndelegate',
            '/osmosis.superfluid.MsgSuperfluidDelegate',
            '/osmosis.superfluid.MsgUnPoolWhitelistedPool'
        )
        AND tx_status = 'SUCCEEDED'

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
msg_att AS (
    SELECT
        tx_id,
        msg_group,
        msg_type,
        attribute_key,
        attribute_value,
        _inserted_timestamp
    FROM
        {{ ref('silver__msg_attributes') }} A
    WHERE
        (
            msg_type IN (
                'lock_tokens',
                'add_tokens_to_lock'
            )
            AND attribute_key IN (
                'period_lock_id',
                'lock_id'
            )
        )
        OR (
            msg_type = 'superfluid_increase_delegation'
            AND attribute_key = 'amount'
        )
        OR (
            attribute_key = 'acc_seq'
        )
),
locks AS (
    SELECT
        b.tx_ID ub_tx_id,
        A.tx_ID,
        attribute_value lock_id
    FROM
        msg_att A
        JOIN (
            SELECT
                DISTINCT lock_id,
                tx_id
            FROM
                base_txn
        ) b
        ON b.lock_id = A.attribute_value
    WHERE
        msg_type IN (
            'lock_tokens',
            'add_tokens_to_lock'
        )
        AND attribute_key IN (
            'period_lock_id',
            'lock_id'
        )
),
lock_body AS (
    SELECT
        b.ub_tx_id,
        b.lock_ID,
        A.tx_id,
        msg :coins [0] :amount AS amount,
        msg :coins [0] :denom AS currency,
        msg :val_addr AS validator_address,
        msg :duration,
        msg AS this
    FROM
        {{ ref('silver__tx_body_msgs') }} A
        JOIN locks b
        ON A.tx_id = b.tx_ID
    WHERE
        msg_type IN (
            '/osmosis.superfluid.MsgLockAndSuperfluidDelegate',
            '/osmosis.superfluid.MsgSuperfluidDelegate'
        )
),
lock_atts AS (
    SELECT
        b.ub_tx_id,
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
        ) AS currency
    FROM
        msg_att A
        JOIN locks b
        ON A.tx_id = b.tx_id
    WHERE
        msg_type = 'superfluid_increase_delegation'
        AND attribute_key = 'amount'
),
lock_ids AS (
    SELECT
        A.tx_id,
        attribute_value AS lock_id
    FROM
        msg_att A
        JOIN base_txn b
        ON A.tx_id = b.tx_id
    WHERE
        msg_type = 'lock_tokens'
        AND attribute_key = 'period_lock_id'

{% if is_incremental() %}
AND A._inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        max_date
)
{% endif %}
),
tx_address AS (
    SELECT
        A.tx_id,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        SPLIT_PART(
            j :acc_seq :: STRING,
            '/',
            0
        ) AS tx_caller_address
    FROM
        msg_att A
        JOIN (
            SELECT
                DISTINCT tx_id
            FROM
                base_txn
        ) b
        ON A.tx_ID = b.tx_ID
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
GROUP BY
    A.tx_id,
    msg_group
)
SELECT
    A.block_id,
    A.block_timestamp,
    A.blockchain,
    chain_ID,
    A.tx_ID,
    A.tx_status,
    tx.tx_caller_address,
    REPLACE(
        A.actio :: STRING,
        '/osmosis.superfluid.Msg'
    ) action,
    A.delegator_address :: STRING AS delegator_address,
    COALESCE(
        A.amount :: INT,
        C.amount :: INT,
        d.amount :: INT
    ) AS amount,
    COALESCE(
        A.currency :: STRING,
        C.currency :: STRING,
        d.currency :: STRING
    ) AS currency,
    CASE
        WHEN COALESCE(
            A.currency :: STRING,
            C.currency :: STRING,
            d.currency :: STRING
        ) LIKE 'gamm/pool/%' THEN 18
        ELSE am.raw_metadata [1] :exponent
    END AS DECIMAL,
    COALESCE(
        A.validator_address :: STRING,
        C.validator_address :: STRING
    ) AS validator_address,
    COALESCE(
        A.lock_id,
        C.lock_id,
        e.lock_id
    ) AS lock_ID,
    C.tx_id AS original_superfluid_delegate_tx_ID,
    _inserted_timestamp,
    concat_ws(
        '-',
        A.tx_id,
        action
    ) AS _unique_key
FROM
    base_txn A
    LEFT JOIN lock_body C
    ON A.tx_id = C.ub_tx_ID
    LEFT JOIN lock_atts d
    ON A.tx_id = d.ub_tx_ID
    LEFT JOIN lock_ids e
    ON A.tx_id = e.tx_id
    LEFT JOIN tx_address tx
    ON A.tx_id = tx.tx_id
    LEFT JOIN {{ ref('silver__asset_metadata') }}
    am
    ON COALESCE(
        A.currency :: STRING,
        C.currency :: STRING,
        d.currency :: STRING
    ) = am.address
