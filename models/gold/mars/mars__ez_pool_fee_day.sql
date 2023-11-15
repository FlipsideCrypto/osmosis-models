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
    block_date,
    pool_id,
    currency,
    b.project_name AS symbol,
    fees,
    fees_usd,
    fee_type
FROM
    {{ ref('silver__pool_fee_summary_day') }} A
    LEFT JOIN {{ ref('silver__asset_metadata') }}
    b
    ON A.currency = b.address
WHERE
    pool_id IN (
        SELECT
            pool_id
        FROM
            pools
    )
