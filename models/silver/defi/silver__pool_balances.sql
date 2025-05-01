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
        DATA :data AS pools,
        VALUE AS value_raw,
        inserted_timestamp AS _INSERTED_TIMESTAMP
    FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_pool_balances') }}
{% else %}
    {{ ref('bronze__streamline_FR_pool_balances') }}
{% endif %}

{% if is_incremental() %}
WHERE
    inserted_timestamp >= (
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
        {# pools, #}
        value_raw :data AS VALUE,
        _INSERTED_TIMESTAMP
    FROM
        sl2 A {# WHERE
        value_raw :BLOCK_NUMBER IS NOT NULL #}
    UNION ALL
    SELECT
        block_id,
        {# pools, #}
        b.value,
        _INSERTED_TIMESTAMP
    FROM
        sl2 A
        JOIN LATERAL FLATTEN(
            A.pools
        ) b {# WHERE
        value_raw :BLOCK_NUMBER IS NULL #}
    UNION ALL
    SELECT
        block_id,
        b.value,
        _INSERTED_TIMESTAMP
    FROM
        base A
        JOIN LATERAL FLATTEN(
            A.pools
        ) b
)
SELECT
    A.block_id,
    C.block_timestamp,
    COALESCE(
        VALUE :"@type",
        VALUE :"type"
    ) :: STRING AS pool_type,
    COALESCE(
        VALUE :address,
        VALUE :chain_model :address
    ) :: STRING AS pool_address,
    COALESCE(
        VALUE :future_pool_governor,
        VALUE :chain_model :future_pool_governor
    ) :: STRING AS future_pool_governor,
    COALESCE(
        VALUE :id,
        VALUE :chain_model :id
    ) :: INT AS pool_id,
    COALESCE(
        VALUE :pool_assets,
        VALUE :poolAssets,
        VALUE :chain_model :pool_assets
    ) AS pool_assets,
    COALESCE(
        VALUE :pool_liquidity,
        VALUE :balances
    ) AS pool_liquidity,
    COALESCE(
        pool_assets [0] :token :amount,
        pool_liquidity [0] :amount
    ) :: bigint AS token_0_amount,
    COALESCE(
        pool_assets [0] :token :denom,
        pool_liquidity [0] :denom
    ) :: STRING AS token_0_denom,
    pool_assets [0] :weight :: bigint AS token_0_weight,
    COALESCE(
        pool_assets [1] :token :amount,
        pool_liquidity [1] :amount
    ) :: bigint AS token_1_amount,
    COALESCE(
        pool_assets [1] :token :denom,
        pool_liquidity [1] :denom
    ) :: STRING AS token_1_denom,
    pool_assets [1] :weight :: bigint AS token_1_weight,
    COALESCE(
        pool_assets [2] :token :amount,
        pool_liquidity [2] :amount
    ) :: bigint AS token_2_amount,
    COALESCE(
        pool_assets [2] :token :denom,
        pool_liquidity [2] :denom
    ) :: STRING AS token_2_denom,
    pool_assets [2] :weight :: bigint AS token_2_weight,
    COALESCE(
        pool_assets [3] :token :amount,
        pool_liquidity [3] :amount
    ) :: bigint AS token_3_amount,
    COALESCE(
        pool_assets [3] :token :denom,
        pool_liquidity [3] :denom
    ) :: STRING AS token_3_denom,
    pool_assets [3] :weight :: bigint AS token_3_weight,
    COALESCE(
        VALUE :pool_params :exit_fee,
        VALUE :poolParams :exitFee,
        VALUE :chain_model :pool_params :exitFee
    ) :: FLOAT AS exit_fee,
    COALESCE(
        VALUE :pool_params :smooth_weight_change_params,
        VALUE :poolParams :smoothWeightChangeParams,
        VALUE :chain_model :pool_params :smooth_weight_change_params
    ) :: STRING AS smooth_weight_change_params,
    COALESCE(
        VALUE :pool_params :swap_fee,
        VALUE :poolParams :swapFee,
        VALUE :chain_model :pool_params :swap_fee
    ) :: FLOAT AS swap_fee,
    COALESCE(
        VALUE :total_shares :amount,
        VALUE :totalShares :amount,
        VALUE :chain_model :total_shares :amount
    ) :: bigint AS total_shares_amount,
    COALESCE(
        VALUE :total_shares :denom,
        VALUE :totalShares :denom,
        VALUE :chain_model :total_shares :denom
    ) :: STRING AS total_shares_denom,
    COALESCE(
        VALUE :total_weight,
        VALUE :totalWeight,
        VALUE :chain_model :totalWeight
    ) :: bigint AS total_weight,
    VALUE :scaling_factor_controller :: STRING AS scaling_factor_controller,
    VALUE :scaling_factors AS scaling_factors,
    VALUE :apr_data AS apr_data,
    VALUE :fees_data AS fees_data,
    A._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['pool_id','a.block_id']
    ) }} AS pool_balances_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    combo A
    JOIN {{ ref('silver__blocks') }} C
    ON A.block_id = C.block_id
WHERE
    pool_id IS NOT NULL qualify (ROW_NUMBER() over (PARTITION BY A.block_id, pool_id
ORDER BY
    A._inserted_timestamp DESC) = 1)
