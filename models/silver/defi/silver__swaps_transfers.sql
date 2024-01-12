{{ config(
    materialized = 'incremental',
    unique_key = ['tx_id','msg_index'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['noncore']
) }}

WITH xfer AS (

    SELECT
        *
    FROM
        {{ ref('silver__transfers_base') }}
    WHERE
        block_timestamp < '2021-09-24 14:11:34.000'
),
matts AS (
    SELECT
        *
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        block_timestamp < '2021-09-24 14:11:34.000'
),
txs AS (
    SELECT
        tx_id
    FROM
        matts
    WHERE
        msg_type = 'message'
        AND attribute_key = 'action'
        AND attribute_value = 'swap_exact_amount_in' qualify(ROW_NUMBER() over(PARTITION BY tx_id
    ORDER BY
        msg_index)) = 1
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
        matts
    WHERE
        msg_type = 'tx'
        AND attribute_key = 'acc_seq' qualify(ROW_NUMBER() over(PARTITION BY tx_id
    ORDER BY
        msg_index)) = 1
),
pools AS (
    SELECT
        pool_address,
        pool_id
    FROM
        {{ ref('silver__pool_metadata') }}
    WHERE
        pool_address IS NOT NULL
),
fin AS (
    SELECT
        A.*,
        COALESCE(
            b_send.pool_id,
            b_rec.pool_id
        ) AS pool_id,
        CASE
            WHEN b_send.pool_address IS NOT NULL THEN 'pool_in_send'
            ELSE 'pool_in_rec'
        END xtype,
        ROW_NUMBER() over (
            PARTITION BY A.tx_id,
            A.sender
            ORDER BY
                A.msg_index
        ) AS send_rank,
        ROW_NUMBER() over (
            PARTITION BY A.tx_id,
            A.receiver
            ORDER BY
                A.msg_index
        ) AS rec_rank
    FROM
        xfer A
        LEFT JOIN pools b_send
        ON A.sender = b_send.pool_address
        LEFT JOIN pools b_rec
        ON A.receiver = b_rec.pool_address
    WHERE
        COALESCE(
            b_send.pool_id,
            b_rec.pool_id
        ) IS NOT NULL
)
SELECT
    A.block_id,
    A.block_timestamp,
    A.tx_id,
    A.tx_succeeded,
    tder.trader,
    A.amount AS from_amount,
    A.currency AS from_currency,
    as_f.decimal AS from_decimal,
    b.amount AS to_amount,
    b.currency AS to_currency,
    as_t.decimal AS TO_DECIMAL,
    A.pool_id,
    A._inserted_timestamp,
    A.msg_index,
    {{ dbt_utils.generate_surrogate_key(
        ['a.tx_id','a.msg_index']
    ) }} AS swaps_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    fin A
    JOIN txs x
    ON A.tx_id = x.tx_id
    JOIN trader tder
    ON A.tx_id = tder.tx_id
    JOIN fin b
    ON A.tx_id = b.tx_id
    AND A.receiver = b.sender
    AND A.rec_rank = b.send_rank
    LEFT OUTER JOIN {{ ref('silver__asset_metadata') }}
    as_f
    ON A.currency = as_f.address
    LEFT OUTER JOIN {{ ref('silver__asset_metadata') }}
    as_t
    ON b.currency = as_t.address
WHERE
    A.xtype = 'pool_in_rec'
    AND b.xtype = 'pool_in_send'
