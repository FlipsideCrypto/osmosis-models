{{ config(
    materialized = 'incremental',
    unique_key = ["pool_id","block_id"],
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp']
) }}

SELECT
    A.block_id,
    C.block_timestamp,
    b.value :"@type" :: STRING AS pool_type,
    b.value :address :: STRING AS pool_address,
    b.value :future_pool_governor :: STRING AS future_pool_governor,
    b.value :id :: INT AS pool_id,
    COALESCE(
        b.value :pool_assets [0] :token :amount,
        b.value :poolAssets [0] :token :amount,
        b.value :pool_liquidity [0] :amount
    ) :: bigint AS token_0_amount,
    COALESCE(
        b.value :pool_assets [0] :token :denom,
        b.value :poolAssets [0] :token :denom,
        b.value :pool_liquidity [0] :denom
    ) :: STRING AS token_0_denom,
    COALESCE(
        b.value :pool_assets [0] :weight,
        b.value :poolAssets [0] :weight
    ) :: bigint AS token_0_weight,
    COALESCE(
        b.value :pool_assets [1] :token :amount,
        b.value :poolAssets [1] :token :amount,
        b.value :pool_liquidity [1] :amount
    ) :: bigint AS token_1_amount,
    COALESCE(
        b.value :pool_assets [1] :token :denom,
        b.value :poolAssets [1] :token :denom,
        b.value :pool_liquidity [1] :denom
    ) :: STRING AS token_1_denom,
    COALESCE(
        b.value :pool_assets [1] :weight,
        b.value :poolAssets [1] :weight
    ) :: bigint AS token_1_weight,
    COALESCE(
        b.value :pool_assets [2] :token :amount,
        b.value :poolAssets [2] :token :amount,
        b.value :pool_liquidity [2] :amount
    ) :: bigint AS token_2_amount,
    COALESCE(
        b.value :pool_assets [2] :token :denom,
        b.value :poolAssets [2] :token :denom,
        b.value :pool_liquidity [2] :denom
    ) :: STRING AS token_2_denom,
    COALESCE(
        b.value :pool_assets [2] :weight,
        b.value :poolAssets [2] :weight
    ) :: bigint AS token_2_weight,
    COALESCE(
        b.value :pool_assets [3] :token :amount,
        b.value :poolAssets [3] :token :amount,
        b.value :pool_liquidity [3] :amount
    ) :: bigint AS token_3_amount,
    COALESCE(
        b.value :pool_assets [3] :token :denom,
        b.value :poolAssets [3] :token :denom,
        b.value :pool_liquidity [3] :denom
    ) :: STRING AS token_3_denom,
    COALESCE(
        b.value :pool_assets [3] :weight,
        b.value :poolAssets [3] :weight
    ) :: bigint AS token_3_weight,
    COALESCE(
        b.value :pool_params :exit_fee,
        b.value :poolParams :exitFee
    ) :: FLOAT AS exit_fee,
    COALESCE(
        b.value :pool_params :smooth_weight_change_params,
        b.value :poolParams :smoothWeightChangeParams
    ) :: STRING AS smooth_weight_change_params,
    COALESCE(
        b.value :pool_params :swap_fee,
        b.value :poolParams :swapFee
    ) :: FLOAT AS swap_fee,
    COALESCE(
        b.value :total_shares :amount,
        b.value :totalShares :amount
    ) :: bigint AS total_shares_amount,
    COALESCE(
        b.value :total_shares :denom,
        b.value :totalShares :denom
    ) :: STRING AS total_shares_denom,
    COALESCE(
        b.value :total_weight,
        b.value :totalWeight
    ) :: bigint AS total_weight,
    b.value :scaling_factor_controller :: STRING AS scaling_factor_controller,
    b.value :scaling_factors AS scaling_factors,
    _INSERTED_DATE AS _inserted_timestamp
FROM
    {{ source(
        'bronze_streamline',
        'pool_balances_api'
    ) }} A
    JOIN LATERAL FLATTEN(
        A.pools
    ) b
    JOIN {{ ref('silver__blocks') }} C
    ON A.block_id = C.block_id

{% if is_incremental() %}
WHERE
    _INSERTED_DATE >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}

qualify (ROW_NUMBER() over (PARTITION BY A.block_id, pool_id
ORDER BY
    _inserted_timestamp DESC) = 1)
