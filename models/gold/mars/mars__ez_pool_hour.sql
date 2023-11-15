{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEFI' }} },
    tags = ['noncore']
) }}

WITH pools AS (

    SELECT
        pool_id
    FROM
        {{ ref('silver__pool_metadata') }},
        LATERAL FLATTEN(assets)
    WHERE
        VALUE :asset_address = 'ibc/573FCD90FACEE750F55A8864EF7D38265F07E5A9273FA0E8DAFD39951332B580'
)
SELECT
    block_id,
    block_timestamp,
    pool_id,
    token_0_denom AS token_0_currency,
    b.project_name AS token_0_symbol,
    token_0_amount,
    token_1_denom AS token_1_currency,
    C.project_name AS token_1_symbol,
    token_1_amount,
    price,
    price_usd,
    twap,
    twap_usd,
    liquidity,
    liquidity_usd,
    volume,
    volume_usd
FROM
    {{ ref('silver__pool_summary_hour') }} A
    LEFT JOIN {{ ref('silver__asset_metadata') }}
    b
    ON A.token_0_denom = b.address
    LEFT JOIN {{ ref('silver__asset_metadata') }} C
    ON A.token_1_denom = C.address
WHERE
    pool_id IN (
        SELECT
            pool_id
        FROM
            pools
    )
