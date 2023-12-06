{{ config(
    materialized = 'table',
    tags = ['noncore']
) }}

SELECT
    recorded_hour AS recorded_at,
    symbol,
    NULL AS currency,
    CLOSE AS price,
    NULL AS total_supply,
    NULL AS volume_24h,
    'coin gecko' AS provider,
    COALESCE(
        token_prices_coin_gecko_hourly_id,
        {{ dbt_utils.generate_surrogate_key(
            ['_unique_key']
        ) }}
    ) AS prices_combined_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__token_prices_coin_gecko_hourly') }}
UNION ALL
SELECT
    recorded_hour AS recorded_at,
    symbol,
    NULL AS currency,
    CLOSE AS price,
    NULL AS total_supply,
    NULL AS volume_24h,
    'coin market cap' AS provider,
    COALESCE(
        token_prices_coin_market_cap_hourly_id,
        {{ dbt_utils.generate_surrogate_key(
            ['_unique_key']
        ) }}
    ) AS prices_combined_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__token_prices_coin_market_cap_hourly') }}
UNION ALL
SELECT
    block_hour AS recorded_at,
    project_name AS symbol,
    currency,
    price_usd AS price,
    NULL AS total_supply,
    NULL AS volume_24h,
    'swaps' AS provider,
    COALESCE(
        prices_swaps_id,
        {{ dbt_utils.generate_surrogate_key(
            ['block_hour','currency']
        ) }}
    ) AS prices_combined_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__prices_swaps') }}
UNION ALL
SELECT
    DATE_TRUNC(
        'hour',
        block_timestamp
    ) AS recorded_at,
    project_name AS symbol,
    token_address AS currency,
    price_usd AS price,
    NULL AS total_supply,
    NULL AS volume_24h,
    'pool balance' AS provider,
    COALESCE(
        pool_token_prices_usd_id,
        {{ dbt_utils.generate_surrogate_key(
            ['token_address','block_id']
        ) }}
    ) AS prices_combined_id,
    COALESCE(
        A.inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        A.modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__pool_token_prices_usd') }} A
    LEFT JOIN {{ ref('silver__asset_metadata') }}
    b
    ON A.token_address = b.address
