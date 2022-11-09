{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
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

swaps AS (
  SELECT 
    block_id, 
    block_timestamp, 
    t.blockchain, 
    chain_id, 
    tx_id, 
    tx_status,
    tx_body,
    _inserted_timestamp, 
    ROW_NUMBER() OVER (
    PARTITION BY tx_id 
    ORDER BY _inserted_timestamp ASC
  ) - 1 AS index
FROM osmosis.silver.transactions t,
LATERAL FLATTEN (input => tx_body :messages, recursive => TRUE ) b 
WHERE 
    key = '@type'
    AND value :: STRING = '/osmosis.gamm.v1beta1.MsgSwapExactAmountIn'
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
pre_final AS (
    SELECT
        block_id,
        block_timestamp,
        blockchain,
        chain_id,
        tx_id,
        tx_status,
        b.value,
        b.value :sender :: STRING AS trader,
        COALESCE(
            b.value :tokenIn :amount :: NUMBER,
            b.value :token_in :amount :: NUMBER
        ) AS from_amount,
        COALESCE(
            b.value :tokenIn :denom :: STRING,
            b.value :token_in :denom :: STRING
        ) AS from_currency,
        COALESCE(
            b.value :tokenOutMinAmount :: NUMBER,
            b.value :token_out_min_amount :: NUMBER
        ) AS to_amount,
        COALESCE(
            b.value :routes [s.index] :tokenOutDenom :: STRING,
            b.value :routes [s.index] :token_out_denom :: STRING
        ) AS to_currency,
        b.value :routes AS routes,
        _inserted_timestamp,
        s.index AS _swap_index
    FROM
        swaps s,
        TABLE(FLATTEN(tx_body :messages)) b
    WHERE
        s.index = b.index
        AND from_amount IS NOT NULL
),
pools AS (
    SELECT
        tx_id,
        _swap_index,
        ARRAY_AGG(
            r.value :poolId
        ) AS pool_ids
    FROM
        pre_final p,
        TABLE(FLATTEN(routes)) r
    GROUP BY
        tx_id,
        _swap_index)
    SELECT
        block_id,
        block_timestamp,
        p.blockchain,
        chain_id,
        p.tx_id,
        tx_status,
        trader,
        from_amount,
        from_currency,
        CASE
            WHEN from_currency LIKE 'gamm/pool/%' THEN 18
            ELSE l.raw_metadata [1] :exponent
        END AS from_decimal,
        to_amount,
        to_currency,
        CASE
            WHEN to_currency LIKE 'gamm/pool/%' THEN 18
            ELSE A.raw_metadata [1] :exponent
        END AS TO_DECIMAL,
        pool_ids,
        _inserted_timestamp
    FROM
        pre_final p
        LEFT OUTER JOIN pools pp
        ON p.tx_id = pp.tx_id
        AND p._swap_index = pp._swap_index
        LEFT OUTER JOIN {{ ref('silver__asset_metadata') }}
        l
        ON from_currency = l.address
        LEFT OUTER JOIN {{ ref('silver__asset_metadata') }} A
        ON to_currency = A.address
