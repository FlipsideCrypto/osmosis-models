{{ config(
    materialized = 'incremental',
    unique_key = ["block_date","pool_id","currency"],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_date'],
    tags = ['noncore']
) }}

WITH last_block_of_day AS (

    SELECT
        block_timestamp :: DATE AS block_date,
        MAX(block_id) AS block_id
    FROM
        {{ ref('silver__blocks') }}
    WHERE
        block_id >= 2300000
    GROUP BY
        block_date
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
    block_timestamp :: DATE >= (
        SELECT
            MAX(
                block_date
            )
        FROM
            {{ this }}
    ) - INTERVAL '7 days'
{% endif %}
),
fees AS (
    SELECT
        block_timestamp :: DATE block_date,
        swap_fee,
        exit_fee,
        pool_id
    FROM
        {{ ref('silver__pool_balances') }}

{% if is_incremental() %}
WHERE
    block_timestamp :: DATE >= (
        SELECT
            MAX(
                block_date
            )
        FROM
            {{ this }}
    ) - INTERVAL '7 days'
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY pool_id, block_timestamp :: DATE
ORDER BY
    block_id) = 1)
),
pre_agg AS (
    SELECT
        A.pool_id,
        b.block_date,
        b.block_id,
        A.from_currency AS currency,
        COALESCE(
            from_amount / pow(
                10,
                CASE
                    WHEN from_currency LIKE 'gamm/pool/%' THEN 18
                    ELSE am.decimal
                END
            ),
            from_amount
        ) * COALESCE(
            fees.swap_fee,
            0
        ) AS fees,
        fees * price_usd AS fees_usd,
        'swap' AS fee_type,
        A._inserted_timestamp
    FROM
        {{ ref('silver__token_swapped') }} A
        LEFT JOIN fees
        ON A.pool_id = fees.pool_id
        AND A.block_timestamp :: DATE = fees.block_date
        JOIN last_block_of_day b
        ON A.block_timestamp :: DATE = b.block_date
        LEFT JOIN {{ ref('silver__asset_metadata') }}
        am
        ON A.from_currency = am.address
        LEFT JOIN pool_token_prices_usd prices
        ON A.from_currency = prices.token_address
        AND A.block_id_hour = prices.block_id
    WHERE
        A.block_id >= 2300000
        AND A.pool_id IS NOT NULL

{% if is_incremental() %}
AND b.block_date >= (
    SELECT
        MAX(
            block_date
        )
    FROM
        {{ this }}
) - INTERVAL '7 days'
{% endif %}
UNION ALL
SELECT
    A.pool_id,
    b.block_date,
    b.block_id,
    A.currency,
    amount / pow(
        10,
        18
    ) * COALESCE(
        fees.exit_fee,
        0
    ) AS fees,
    NULL AS fees_usd,
    'exit' AS fee_type,
    A._inserted_timestamp
FROM
    {{ ref('silver__liquidity_provider_actions') }} A
    JOIN fees
    ON A.pool_id = fees.pool_id
    AND A.block_timestamp :: DATE = fees.block_date
    JOIN last_block_of_day b
    ON A.block_timestamp :: DATE = b.block_date
WHERE
    A.action = 'lp_tokens_burned'
    AND COALESCE(
        fees.exit_fee,
        0
    ) <> 0
    AND A.block_id >= 2300000

{% if is_incremental() %}
AND b.block_date >= (
    SELECT
        MAX(
            block_date
        )
    FROM
        {{ this }}
) - INTERVAL '7 days'
{% endif %}
)
SELECT
    pool_id,
    block_date,
    block_id,
    currency,
    SUM(fees) AS fees,
    SUM(fees_usd) AS fees_usd,
    fee_type,
    MAX(_inserted_timestamp) AS _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['pool_id','block_date','currency']
    ) }} AS pool_fee_summary_day_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_agg A
GROUP BY
    pool_id,
    block_date,
    block_id,
    currency,
    fee_type
