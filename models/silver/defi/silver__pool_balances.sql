{{ config(
    materialized = 'incremental',
    unique_key = ["pool_id","block_id"],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp'],
    tags = ['noncore']
) }}
-- depends_on: {{ ref('bronze__streamline_pool_balances') }}
WITH base AS (

    SELECT
        A.block_id,
        A.pools,
        A._INSERTED_DATE AS _INSERTED_TIMESTAMP
    FROM
        {{ source(
            'bronze_streamline',
            'pool_balances_api'
        ) }} A

{% if is_incremental() %}
WHERE
    0 = 1
{% endif %}
),
sl2 AS (
    SELECT
        VALUE :metadata :request :headers :"x-cosmos-block-height" :: INT AS block_id,
        DATA :pools AS pools,
        _INSERTED_TIMESTAMP
    FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_pool_balances') }}
{% else %}
    {{ ref('bronze__streamline_FR_pool_balances') }}
{% endif %}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
combo AS (
    SELECT
        block_id,
        pools,
        _INSERTED_TIMESTAMP
    FROM
        sl2
    UNION ALL
    SELECT
        block_id,
        pools,
        _INSERTED_TIMESTAMP
    FROM
        base
)
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
    A._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['pool_id','a.block_id']
    ) }} AS pool_balances_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    combo A
    JOIN LATERAL FLATTEN(
        A.pools
    ) b
    JOIN {{ ref('silver__blocks') }} C
    ON A.block_id = C.block_id qualify (ROW_NUMBER() over (PARTITION BY A.block_id, pool_id
ORDER BY
    A._inserted_timestamp DESC) = 1)
