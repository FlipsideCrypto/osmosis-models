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
        top 100 block_ID,
        block_timestamp,
        blockchain,
        chain_id,
        tx_id,
        tx_status,
        VALUE AS actio,
        REPLACE(LEFT(path, CHARINDEX(path, ']')), '[') AS msg_group,
        this :sender AS delegator_address,
        this :coins [0] :amount AS amount,
        this :coins [0] :denom AS currency,
        this :val_addr AS validator_address,
        this :duration,
        _inserted_timestamp,
        this :lock_id AS lock_id
    FROM
        {{ ref('silver__transactions') }} A,
        LATERAL FLATTEN (
            input => tx_body :messages,
            recursive => TRUE
        ) b
    WHERE
        key = '@type'
        AND VALUE :: STRING IN (
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
locks AS (
    SELECT
        b.tx_ID ub_tx_id,
        A.tx_ID,
        attribute_value lock_id
    FROM
        {{ ref('silver__msg_attributes') }} A
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
        AND attribute_key LIKE '%lock%'

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
lock_body AS (
    SELECT
        b.ub_tx_id,
        b.lock_ID,
        A.tx_id,
        this :coins [0] :amount AS amount,
        this :coins [0] :denom AS currecy,
        this :val_addr AS validator_address,
        this :duration,
        this
    FROM
        {{ ref('silver__transactions') }} A,
        locks b,
        LATERAL FLATTEN (
            input => tx_body :messages,
            recursive => TRUE
        ) C
    WHERE
        A.tx_id = b.tx_ID
        AND key = '@type'
        AND VALUE :: STRING IN (
            '/osmosis.superfluid.MsgLockAndSuperfluidDelegate',
            '/osmosis.superfluid.MsgSuperfluidDelegate'
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
        {{ ref('silver__msg_attributes') }} A
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
        C.amount :: INT
    ) AS amount,
    A.currency :: STRING AS currency,
    CASE
        WHEN A.currency LIKE 'gamm/pool/%' THEN 18
        ELSE am.raw_metadata [1] :exponent
    END AS DECIMAL,
    COALESCE(
        A.validator_address :: STRING,
        C.validator_address :: STRING
    ) AS validator_address,
    COALESCE(
        A.lock_id,
        C.lock_id
    ) AS lock_ID,
    C.tx_ID AS original_superfluid_delegate_tx_ID,
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
    LEFT JOIN tx_address tx
    ON A.tx_id = tx.tx_id
    LEFT JOIN {{ ref('silver__asset_metadata') }}
    am
    ON A.currency = am.address
