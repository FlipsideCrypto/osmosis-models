{{ config(
    materialized = 'incremental',
    unique_key = ["token_address","block_id"],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp::DATE'],
    tags = ['noncore']
) }}

WITH osmo_price AS (

    SELECT
        block_id,
        block_timestamp,
        price AS osmo_price,
        price_denom token_address,
        pool_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__pool_token_prices') }} A
    WHERE
        pool_id = 1464

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
),
osmo_pools AS (
    SELECT
        osmo_price / price AS price_usd,
        A.block_id,
        A.block_timestamp,
        A.token_address,
        A.pool_total,
        A.pool_id,
        A._inserted_timestamp
    FROM
        {{ ref('silver__pool_token_prices') }} A
        JOIN osmo_price b
        ON A.block_id = b.block_id
    WHERE
        price_denom = 'uosmo'
        AND token_pool_rank = 1

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
non_osomo_price AS (
    SELECT
        A.token_address,
        A.price_usd,
        A.block_id
    FROM
        osmo_pools A qualify(ROW_NUMBER() over(PARTITION BY A.token_address
    ORDER BY
        pool_total DESC) = 1)
),
non_osmo_pools AS (
    SELECT
        b.price_usd / price AS price_usd,
        A.block_id,
        A.block_timestamp,
        A.token_address,
        A.pool_id,
        A._inserted_timestamp
    FROM
        {{ ref('silver__pool_token_prices') }} A
        JOIN non_osomo_price b
        ON A.block_id = b.block_id
        AND A.price_denom = b.token_address
        LEFT JOIN osmo_pools ex
        ON A.token_address = ex.token_address
        AND A.block_id = ex.block_id
    WHERE
        price_denom <> 'uosmo'
        AND A.token_address <> 'uosmo'
        AND ex.token_address IS NULL

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

qualify(ROW_NUMBER() over(PARTITION BY A.token_address
ORDER BY
    A.pool_total DESC) = 1)
),
stable_pools AS (
    SELECT
        *
    FROM
        {{ ref('silver__pool_balances') }}
    WHERE
        pool_type ILIKE '%stable%'

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
),
adjust_stable AS (
    SELECT
        block_id,
        block_timestamp,
        pool_id,
        _inserted_timestamp,
        token_0_amount / scaling_factors [0] :: INT AS amount,
        scaling_factors [0] :: INT AS sf,
        token_0_denom AS denom
    FROM
        stable_pools A
    WHERE
        token_0_amount IS NOT NULL
    UNION ALL
    SELECT
        block_id,
        block_timestamp,
        pool_id,
        _inserted_timestamp,
        token_1_amount / scaling_factors [1] :: INT AS amount,
        scaling_factors [1] :: INT AS sf,
        token_1_denom
    FROM
        stable_pools A
    WHERE
        token_1_amount IS NOT NULL
    UNION ALL
    SELECT
        block_id,
        block_timestamp,
        pool_id,
        _inserted_timestamp,
        token_2_amount / scaling_factors [2] :: INT AS amount,
        scaling_factors [2] :: INT AS sf,
        token_2_denom
    FROM
        stable_pools A
    WHERE
        token_2_amount IS NOT NULL
    UNION ALL
    SELECT
        block_id,
        block_timestamp,
        pool_id,
        _inserted_timestamp,
        token_3_amount / scaling_factors [3] :: INT AS amount,
        scaling_factors [3] :: INT AS sf,
        token_3_denom
    FROM
        stable_pools A
    WHERE
        token_3_amount IS NOT NULL
),
no_price AS (
    SELECT
        A.block_id,
        A.block_timestamp,
        A.pool_id,
        A.amount,
        A.sf,
        A.denom,
        A._INSERTED_timestamp
    FROM
        adjust_stable A
        LEFT JOIN (
            SELECT
                block_id,
                token_address
            FROM
                osmo_price
            UNION ALL
            SELECT
                block_id,
                token_address
            FROM
                osmo_pools
            UNION ALL
            SELECT
                block_id,
                token_address
            FROM
                non_osmo_pools
        ) b
        ON A.block_id = b.block_id
        AND A.denom = b.token_address
    WHERE
        b.block_id IS NULL qualify(ROW_NUMBER() over(PARTITION BY A.denom, A.block_id
    ORDER BY
        A.amount DESC) = 1)
),
yes_price AS (
    SELECT
        A.block_id,
        A.pool_id,
        b.price_usd,
        b.token_address
    FROM
        adjust_stable A
        JOIN (
            SELECT
                block_id,
                token_address,
                osmo_price AS price_usd
            FROM
                osmo_price
            UNION ALL
            SELECT
                block_id,
                token_address,
                price_usd
            FROM
                osmo_pools
            UNION ALL
            SELECT
                block_id,
                token_address,
                price_usd
            FROM
                non_osmo_pools
        ) b
        ON A.block_id = b.block_id
        AND A.denom = b.token_address qualify(ROW_NUMBER() over(PARTITION BY A.pool_id, A.block_id
    ORDER BY
        A.amount DESC) = 1)
),
stable_prices AS (
    SELECT
        A.block_id,
        A.block_timestamp,
        A.denom AS token_address,
        (
            sf2.sf / sf1.sf
        ) * b.price_usd AS price_usd,
        A.pool_id,
        A._inserted_timestamp
    FROM
        no_price A
        JOIN yes_price b
        ON A.block_id = b.block_id
        AND A.pool_id = b.pool_id
        LEFT JOIN adjust_stable sf1
        ON A.block_id = sf1.block_id
        AND A.pool_id = sf1.pool_id
        AND A.denom = sf1.denom
        LEFT JOIN adjust_stable sf2
        ON b.block_id = sf2.block_id
        AND b.pool_id = sf2.pool_id
        AND b.token_address = sf2.denom
),
fin AS (
    SELECT
        block_id,
        block_timestamp,
        token_address,
        osmo_price AS price_usd,
        pool_id,
        _inserted_timestamp
    FROM
        osmo_price
    UNION ALL
    SELECT
        block_id,
        block_timestamp,
        token_address,
        price_usd,
        pool_id,
        _inserted_timestamp
    FROM
        osmo_pools
    UNION ALL
    SELECT
        block_id,
        block_timestamp,
        token_address,
        price_usd,
        pool_id,
        _inserted_timestamp
    FROM
        non_osmo_pools
    UNION ALL
    SELECT
        block_id,
        block_timestamp,
        token_address,
        price_usd,
        pool_id,
        _inserted_timestamp
    FROM
        stable_prices
)
SELECT
    block_id,
    block_timestamp,
    token_address,
    price_usd,
    pool_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['token_address','block_id']
    ) }} AS pool_token_prices_usd_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    fin qualify ROW_NUMBER() over(
        PARTITION BY block_id,
        token_address
        ORDER BY
            pool_id DESC
    ) = 1
