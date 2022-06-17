{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, action)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}
--need to add incremental logic
---all relevant tx_ids
WITH base_txn AS (

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
        _INGESTED_AT,
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
        AND _ingested_at :: DATE >= CURRENT_DATE - 2
        {% endif %}
),
--find the relevant lock ids for undelegate events (need to be able to look up the validator)
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
AND _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
),
--get the body info from the original delegate
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
    AND _ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
), --tie together the delegate events with the undelegte (plus the info from the original delegate)

-- caller logic 
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
AND _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
GROUP BY
    A.tx_id,
    msg_group
)

--need to all the caller logic from the other staking model
SELECT
    A.block_id,
    A.block_timestamp,
    blockchain,
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
    COALESCE(
        A.validator_address :: STRING,
        C.validator_address :: STRING
    ) AS validator_address,
    COALESCE(
        A.lock_id,
        C.lock_id
    ) AS lock_ID,
    C.tx_ID AS original_superfluid_delegate_tx_ID, 
    _ingested_at
FROM
    base_txn A

    LEFT JOIN lock_body C
    ON A.tx_id = C.ub_tx_ID

    LEFT JOIN tx_address tx
    ON A.tx_id = tx.tx_id
