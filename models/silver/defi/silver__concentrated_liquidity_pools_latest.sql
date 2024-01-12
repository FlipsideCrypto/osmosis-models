{{ config(
    materialized = 'incremental',
    unique_key = 'pool_id',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['noncore']
) }}

WITH base AS (

    SELECT
        resp,
        _inserted_timestamp
    FROM
        {{ ref(
            'bronze_api__concentrated_liquidity_pools'
        ) }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
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
    TYPE,
    address,
    incentives_address,
    spread_rewards_address,
    pool_id,
    current_tick_liquidity,
    token0,
    token1,
    token2,
    token3,
    token4,
    ARRAY_CONSTRUCT_COMPACT (OBJECT_CONSTRUCT('asset_addres', token0),CASE
    WHEN token1 IS NOT NULL THEN OBJECT_CONSTRUCT('asset_addres', token1)END,CASE
    WHEN token2 IS NOT NULL THEN OBJECT_CONSTRUCT('asset_addres', token2)END,CASE
    WHEN token3 IS NOT NULL THEN OBJECT_CONSTRUCT('asset_addres', token3)END,CASE
    WHEN token4 IS NOT NULL THEN OBJECT_CONSTRUCT('asset_addres', token4)END) AS assets,
    current_sqrt_price,
    current_tick,
    tick_spacing,
    exponent_at_price_one,
    spread_factor,
    last_liquidity_update,
    {{ dbt_utils.generate_surrogate_key(
        ['pool_id']
    ) }} AS concentrated_liquidity_pools_latest_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref(
        'silver__concentrated_liquidity_pools'
    ) }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY pool_id
ORDER BY
    _inserted_timestamp DESC)) = 1
