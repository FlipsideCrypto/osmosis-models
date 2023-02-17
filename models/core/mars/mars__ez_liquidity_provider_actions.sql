{{ config(
    materialized = 'view'
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
    tx_id,
    tx_succeeded,
    liquidity_provider_address,
    action,
    pool_id,
    COALESCE(
        amount / pow(
            10,
            CASE
                WHEN amount LIKE 'gamm/pool/%' THEN 18
                ELSE b.decimal
            END
        ),
        amount
    ) AS amount,
    currency,
    b.project_name AS symbol
FROM
    {{ ref('silver__liquidity_provider_actions') }} A
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
