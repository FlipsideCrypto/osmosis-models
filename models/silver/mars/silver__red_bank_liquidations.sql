{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE'],
    enabled = False
) }}

WITH tx AS (

    SELECT
        DISTINCT A.block_timestamp,
        A.tx_id
    FROM
        {{ ref('silver__msg_attributes') }} A
    WHERE
        block_timestamp :: DATE >= '2023-02-13'
        AND attribute_key = 'action'
        AND attribute_value = 'liquidate'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) _inserted_timestamp
    FROM
        {{ this }}
)
{% endif %}
),
relevant_msgs AS (
    SELECT
        A.block_id,
        A.block_timestamp,
        A.tx_id,
        tx_succeeded,
        A.msg_group,
        A.msg_sub_group,
        A.msg_index,
        A.msg_type,
        attribute_index,
        attribute_key,
        attribute_value,
        _inserted_timestamp
    FROM
        {{ ref('silver__msg_attributes') }} A
        JOIN tx b
        ON A.tx_id = b.tx_id
        AND A.block_timestamp :: DATE = b.block_timestamp :: DATE
    WHERE
        A.block_timestamp :: DATE >= '2023-02-13'
        AND msg_type IN (
            'transfer',
            'wasm'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) _inserted_timestamp
    FROM
        {{ this }}
)
{% endif %}
),
filter_msgs AS (
    SELECT
        A.block_id,
        A.block_timestamp,
        A.tx_id,
        tx_succeeded,
        A.msg_group,
        A.msg_sub_group,
        A.msg_index,
        A.msg_type,
        attribute_index,
        attribute_key,
        attribute_value,
        _inserted_timestamp
    FROM
        relevant_msgs A
        LEFT JOIN (
            SELECT
                tx_id,
                msg_index
            FROM
                relevant_msgs
            WHERE
                attribute_key = 'action'
                AND attribute_value = 'balance_change'
        ) b
        ON A.tx_id = b.tx_id
        AND A.msg_index = b.msg_index
    WHERE
        b.tx_id IS NULL
),
first_agg AS (
    SELECT
        A.block_id,
        A.block_timestamp,
        A.tx_id,
        tx_succeeded,
        msg_group,
        msg_index,
        msg_type,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,CASE
            WHEN j :action :: STRING IS NOT NULL THEN TRUE
            ELSE FALSE
        END action,
        NULLIF(
            (conditional_true_event(action) over (PARTITION BY tx_id
            ORDER BY
                msg_index) -1),
                -1
        ) abc,CASE
            WHEN msg_group IS NULL THEN 'fee'
            WHEN abc IS NULL
            AND msg_type = 'transfer' THEN 'sent'
            WHEN msg_type = 'wasm' THEN 'liq'
            WHEN abc IS NOT NULL THEN 'rec'
        END msg_index_desc,
        _inserted_timestamp
    FROM
        filter_msgs A
    GROUP BY
        block_id,
        A.block_timestamp,
        A.tx_id,
        tx_succeeded,
        msg_group,
        msg_index,
        msg_type,
        _inserted_timestamp
)
SELECT
    A.block_id,
    A.block_timestamp,
    A.tx_id,
    tx_succeeded,
    A.j :amount :: STRING AS fee_amount,
    A.j :sender :: STRING AS fee_sender,
    b.liq_sent_amount,
    liq_sent_sender,
    collateral_amount,
    collateral_amount_scaled,
    collateral_denom,
    debt_amount,
    debt_amount_scaled,
    debt_denom,
    liquidator,
    recipient,
    USER,
    liq_rec_payment,
    liq_rec_sender,
    _inserted_timestamp
FROM
    first_agg A
    JOIN (
        SELECT
            block_id,
            block_timestamp,
            tx_id,
            j :amount :: STRING AS liq_sent_amount,
            j :sender :: STRING AS liq_sent_sender
        FROM
            first_agg
        WHERE
            msg_index_desc = 'sent'
    ) b
    ON A.tx_id = b.tx_id
    JOIN (
        SELECT
            tx_id,
            j :collateral_amount :: INT AS collateral_amount,
            j :collateral_amount_scaled :: INT AS collateral_amount_scaled,
            j :collateral_denom :: STRING AS collateral_denom,
            j :debt_amount :: INT AS debt_amount,
            j :debt_amount_scaled :: INT AS debt_amount_scaled,
            j :debt_denom :: STRING AS debt_denom,
            j :liquidator :: STRING AS liquidator,
            j :recipient :: STRING AS recipient,
            j :user :: STRING AS USER
        FROM
            first_agg
        WHERE
            msg_index_desc = 'liq'
    ) C
    ON A.tx_id = C.tx_id
    JOIN (
        SELECT
            tx_id,
            j :amount :: STRING AS liq_rec_payment,
            j :recipient :: STRING AS liq_rec_sender
        FROM
            first_agg
        WHERE
            msg_index_desc = 'rec'
    ) d
    ON A.tx_id = d.tx_id
WHERE
    A.msg_index_desc = 'fee'
