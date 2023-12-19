{{ config(
    materialized = 'incremental',
    unique_key = ['pool_id','_inserted_timestamp'],
    incremental_strategy = 'delete+insert',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp::DATE'],
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
    VALUE :"@type" :: STRING AS TYPE,
    VALUE: "address" :: STRING AS address,
    VALUE: "incentives_address" :: STRING AS incentives_address,
    VALUE :"spread_rewards_address" :: STRING AS spread_rewards_address,
    VALUE :"id" :: INT AS pool_id,
    VALUE :"current_tick_liquidity" :: FLOAT AS current_tick_liquidity,
    VALUE :"token0" :: STRING AS token0,
    VALUE :"token1" :: STRING AS token1,
    VALUE :"token2" :: STRING AS token2,
    VALUE :"token3" :: STRING AS token3,
    VALUE :"token4" :: STRING AS token4,
    VALUE: "current_sqrt_price" :: FLOAT AS current_sqrt_price,
    VALUE :"current_tick" :: bigint AS current_tick,
    VALUE :"tick_spacing" :: INT AS tick_spacing,
    VALUE :"exponent_at_price_one" :: FLOAT AS exponent_at_price_one,
    VALUE :"spread_factor" :: FLOAT AS spread_factor,
    VALUE: "last_liquidity_update" :: datetime AS last_liquidity_update,
    {{ dbt_utils.generate_surrogate_key(
        ['pool_id','_inserted_timestamp']
    ) }} AS concentrated_liquidity_pools_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base,
    LATERAL FLATTEN(
        resp :data :pools
    )
