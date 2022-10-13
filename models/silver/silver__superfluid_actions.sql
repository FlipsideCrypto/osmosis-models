{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['_inserted_timestamp::DATE'],
) }}

SELECT
    block_id,
    block_timestamp,
    blockchain,
    chain_id,
    A.tx_id,
    tx_status,
    msg_type,
    A.msg_group,
    msg :sender :: STRING AS delegator_address,
    msg :coins [0] :amount :: INT AS amount,
    msg :coins [0] :denom :: STRING AS currency,
    msg :val_addr :: STRING AS validator_address,
    COALESCE(
        msg :lock_id :: INT,
        b.lock_id :: INT
    ) AS lock_id,
    concat_ws(
        '-',
        A.tx_id,
        A.msg_group
    ) AS _unique_key,
    _inserted_timestamp
FROM
    {{ ref('silver__tx_body_msgs') }} A
    LEFT JOIN (
        SELECT
            tx_id,
            msg_group,
            attribute_value AS lock_id
        FROM
            {{ ref('silver__msg_attributes') }} A
        WHERE
            msg_type IN (
                'lock_tokens',
                'add_tokens_to_lock'
            )
            AND attribute_key IN (
                'period_lock_id',
                'lock_id'
            )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
) b
ON A.tx_id = b.tx_id
AND A.msg_group = b.msg_group
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
        {{ this }}
)
{% endif %}
