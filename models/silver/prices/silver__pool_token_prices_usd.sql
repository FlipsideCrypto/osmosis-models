{{ config(
    materialized = 'incremental',
    unique_key = ["token_address","block_id"],
    incremental_strategy = 'merge',
) }}

WITH osmo_price AS (

    SELECT
        block_id,
        block_timestamp,
        price AS osmo_price,
        price_denom token_address,
        _inserted_timestamp
    FROM
        {{ ref('silver__pool_token_prices') }} A
    WHERE
        price_denom = 'uosmo'
        AND token_address = 'ibc/D189335C6E4A68B513C10AB227BF1C1D38C746766278BA3EEB4FB14124F1D858' --axUSDC

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
        t0.project_name,
        osmo_price / price AS price_usd,
        A.block_id,
        A.block_timestamp,
        A.token_address,
        A._inserted_timestamp
    FROM
        {{ ref('silver__pool_token_prices') }} A
        JOIN osmo_price b
        ON A.block_id = b.block_id
        JOIN silver.asset_metadata t0
        ON A.token_address = t0.address
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
)
SELECT
    block_id,
    block_timestamp,
    token_address,
    osmo_price AS price_usd,
    _inserted_timestamp
FROM
    osmo_price
UNION ALL
SELECT
    block_id,
    block_timestamp,
    token_address,
    price_usd,
    _inserted_timestamp
FROM
    osmo_pools
