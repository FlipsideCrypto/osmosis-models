{{ config(
    materialized = 'incremental',
    unique_key = ["token_address","pool_id","block_id"],
    incremental_strategy = 'merge',
) }}

WITH top_pools AS (

    SELECT
        A.block_id,
        A.block_timestamp,
        token_0_denom,
        COALESCE(
            token_0_amount / pow(
                10,
                CASE
                    WHEN token_0_denom LIKE 'gamm/pool/%' THEN 18
                    ELSE t0.decimal
                END
            ),
            token_0_amount
        ) AS token_0_amount,
        token_1_denom,
        COALESCE(
            token_1_amount / pow(
                10,
                CASE
                    WHEN token_1_denom LIKE 'gamm/pool/%' THEN 18
                    ELSE t1.decimal
                END
            ),
            token_1_amount
        ) AS token_1_amount,
        pool_id,
        A._inserted_timestamp
    FROM
        {{ ref('silver__pool_balances') }} A
        LEFT JOIN {{ ref('silver__asset_metadata') }}
        t0
        ON A.token_0_denom = t0.address
        LEFT JOIN {{ ref('silver__asset_metadata') }}
        t1
        ON A.token_1_denom = t1.address
    WHERE
        pool_type = '/osmosis.gamm.v1beta1.Pool'
        AND token_2_denom IS NULL
        AND (
            t0.decimal IS NOT NULL
            OR token_0_denom LIKE 'gamm/pool/%'
            OR t1.decimal IS NOT NULL
            OR token_1_denom LIKE 'gamm/pool/%'
        )

{% if is_incremental() %}
AND A._inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
),
fin AS (
    SELECT
        block_id,
        block_timestamp,
        token_0_denom AS token_address,
        token_0_amount / token_1_amount AS price,
        token_1_denom AS price_denom,
        pool_id,
        token_0_amount + token_1_amount AS pool_total,
        _inserted_timestamp
    FROM
        top_pools
    UNION ALL
    SELECT
        block_id,
        block_timestamp,
        token_1_denom AS token_address,
        token_1_amount / token_0_amount AS price,
        token_0_denom AS price_denom,
        pool_id,
        token_0_amount + token_1_amount AS pool_total,
        _inserted_timestamp
    FROM
        top_pools
)
SELECT
    block_id,
    block_timestamp,
    token_address,
    price,
    price_denom,
    pool_id,
    pool_total,
    ROW_NUMBER() over(
        PARTITION BY block_id,
        token_address,
        price_denom
        ORDER BY
            pool_total DESC
    ) AS token_pool_rank,
    _inserted_timestamp
FROM
    fin
