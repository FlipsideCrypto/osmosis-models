{{ config(
    materialized = 'incremental',
    unique_key = ["pool_id","block_hour"],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp'],
    tags = ['noncore']
) }}

WITH pool_token_prices AS (

    SELECT
        block_timestamp,
        block_id,
        token_address,
        pool_id,
        price
    FROM
        {{ ref('silver__pool_token_prices') }}

{% if is_incremental() %}
WHERE
    block_timestamp >= (
        SELECT
            MAX(
                block_timestamp
            )
        FROM
            {{ this }}
    ) - INTERVAL '7 days'
{% endif %}
),
pool_token_prices_usd AS (
    SELECT
        block_timestamp,
        block_id,
        token_address,
        price_usd
    FROM
        {{ ref('silver__pool_token_prices_usd') }}

{% if is_incremental() %}
WHERE
    block_timestamp >= (
        SELECT
            MAX(
                block_timestamp
            )
        FROM
            {{ this }}
    ) - INTERVAL '7 days'
{% endif %}
),
twap AS (
    SELECT
        block_date,
        token_address,
        pool_id,
        AVG(price) price
    FROM
        (
            SELECT
                block_timestamp :: DATE block_date,
                token_address,
                pool_id,
                MIN(price) price
            FROM
                pool_token_prices
            GROUP BY
                block_date,
                token_address,
                pool_id
            UNION ALL
            SELECT
                block_timestamp :: DATE block_date,
                token_address,
                pool_id,
                MAX(price) price_usd
            FROM
                pool_token_prices
            GROUP BY
                block_date,
                token_address,
                pool_id
            UNION ALL
            SELECT
                block_timestamp :: DATE,
                token_address,
                pool_id,
                price
            FROM
                pool_token_prices qualify (ROW_NUMBER() over(PARTITION BY block_timestamp :: DATE, token_address
            ORDER BY
                block_id) = 1)
            UNION ALL
            SELECT
                block_timestamp :: DATE,
                token_address,
                pool_id,
                price
            FROM
                pool_token_prices qualify (ROW_NUMBER() over(PARTITION BY block_timestamp :: DATE, token_address
            ORDER BY
                block_id DESC) = 1)
        )
    GROUP BY
        block_date,
        token_address,
        pool_id
),
twap_usd AS (
    SELECT
        block_date,
        token_address,
        AVG(price_usd) price_usd
    FROM
        (
            SELECT
                block_timestamp :: DATE block_date,
                token_address,
                MIN(price_usd) price_usd
            FROM
                pool_token_prices_usd
            GROUP BY
                block_date,
                token_address
            UNION ALL
            SELECT
                block_timestamp :: DATE block_date,
                token_address,
                MAX(price_usd) price_usd
            FROM
                pool_token_prices_usd
            GROUP BY
                block_date,
                token_address
            UNION ALL
            SELECT
                block_timestamp :: DATE,
                token_address,
                price_usd
            FROM
                pool_token_prices_usd qualify (ROW_NUMBER() over(PARTITION BY block_timestamp :: DATE, token_address
            ORDER BY
                block_id) = 1)
            UNION ALL
            SELECT
                block_timestamp :: DATE,
                token_address,
                price_usd
            FROM
                pool_token_prices_usd qualify (ROW_NUMBER() over(PARTITION BY block_timestamp :: DATE, token_address
            ORDER BY
                block_id DESC) = 1)
        )
    GROUP BY
        block_date,
        token_address
),
volume AS (
    SELECT
        block_id_hour,
        pool_id,
        SUM(amount) amount
    FROM
        (
            SELECT
                block_id_hour,
                pool_id,
                to_amount / pow(
                    10,
                    6
                ) amount
            FROM
                {{ ref('silver__token_swapped') }}
            WHERE
                to_currency = 'uosmo'

{% if is_incremental() %}
AND block_timestamp >= (
    SELECT
        MAX(
            block_timestamp
        )
    FROM
        {{ this }}
) - INTERVAL '7 days'
{% endif %}
UNION ALL
SELECT
    block_id_hour,
    pool_id,
    from_amount / pow(
        10,
        6
    ) AS amount
FROM
    {{ ref('silver__token_swapped') }}
WHERE
    from_currency = 'uosmo'

{% if is_incremental() %}
AND block_timestamp >= (
    SELECT
        MAX(
            block_timestamp
        )
    FROM
        {{ this }}
) - INTERVAL '7 days'
{% endif %}
)
GROUP BY
    block_id_hour,
    pool_id
),
adjust AS (
    SELECT
        A.block_id,
        A.block_timestamp,
        A.pool_id,
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
        token_2_denom,
        COALESCE(
            token_2_amount / pow(
                10,
                CASE
                    WHEN token_2_amount LIKE 'gamm/pool/%' THEN 18
                    ELSE t2.decimal
                END
            ),
            token_2_amount
        ) AS token_2_amount,
        token_3_denom,
        COALESCE(
            token_3_amount / pow(
                10,
                CASE
                    WHEN token_3_amount LIKE 'gamm/pool/%' THEN 18
                    ELSE t3.decimal
                END
            ),
            token_3_amount
        ) AS token_3_amount,
        A._inserted_timestamp
    FROM
        {{ ref('silver__pool_balances') }} A
        LEFT JOIN {{ ref('silver__asset_metadata') }}
        t0
        ON A.token_0_denom = t0.address
        LEFT JOIN {{ ref('silver__asset_metadata') }}
        t1
        ON A.token_1_denom = t1.address
        LEFT JOIN {{ ref('silver__asset_metadata') }}
        t2
        ON A.token_2_denom = t2.address
        LEFT JOIN {{ ref('silver__asset_metadata') }}
        t3
        ON A.token_3_denom = t3.address

{% if is_incremental() %}
WHERE
    A.block_timestamp >= (
        SELECT
            MAX(
                block_timestamp
            )
        FROM
            {{ this }}
    ) - INTERVAL '7 days'
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY DATE_TRUNC('hour', block_timestamp), pool_id
ORDER BY
    block_id DESC) = 1)
)
SELECT
    A.block_id,
    A.block_timestamp,
    DATE_TRUNC(
        'hour',
        A.block_timestamp
    ) block_hour,
    A.pool_id,
    token_0_denom,
    token_0_amount,
    token_1_denom,
    token_1_amount,
    b.price,
    C.price_usd,
    twap.price AS twap,
    twap_usd.price_usd AS twap_usd,
    CASE
        WHEN token_1_denom = 'uosmo' THEN token_1_amount * 2
    END AS liquidity,
    CASE
        WHEN token_1_denom = 'uosmo' THEN token_1_amount * 2 * op.price_usd
    END AS liquidity_usd,
    v.amount AS volume,
    v.amount * op.price_usd AS volume_usd,
    token_2_denom,
    token_2_amount,
    token_3_denom,
    token_3_amount,
    A._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['a.pool_id','block_hour']
    ) }} AS pool_summary_hour_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    adjust A
    LEFT JOIN pool_token_prices b
    ON A.block_id = b.block_id
    AND A.pool_id = b.pool_id
    AND A.token_0_denom = b.token_address
    AND token_2_denom IS NULL
    LEFT JOIN pool_token_prices_usd C
    ON A.block_id = C.block_id
    AND A.token_0_denom = C.token_address
    JOIN pool_token_prices_usd op
    ON A.block_id = op.block_id
    AND op.token_address = 'uosmo'
    LEFT JOIN twap
    ON A.token_0_denom = twap.token_address
    AND A.block_timestamp :: DATE = twap.block_date
    AND A.pool_id = twap.pool_id
    LEFT JOIN twap_usd
    ON A.token_0_denom = twap_usd.token_address
    AND A.block_timestamp :: DATE = twap_usd.block_date
    LEFT JOIN volume v
    ON A.block_id = v.block_id_hour
    AND A.pool_id = v.pool_id
