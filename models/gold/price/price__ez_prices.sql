{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'PRICES' }} },
    tags = ['noncore']
) }}

WITH p_base AS (

    SELECT
        DATE_TRUNC(
            'hour',
            recorded_at
        ) AS recorded_hour,
        UPPER(
            A.symbol
        ) AS symbol,
        currency,
        A.price,
        A.provider,
        CASE
            A.provider
            WHEN 'coin gecko' THEN 1
            WHEN 'coin market cap' THEN 2
            WHEN 'pool balances' THEN 3
            ELSE 4
        END AS pro_rank,
        dim_prices_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('price__dim_prices') }} A
    WHERE
        A.provider <> 'swaps'
)
SELECT
    A.recorded_hour,
    A.symbol,
    COALESCE(
        A.currency,
        b_1.address,
        b_2.address
    ) AS currency,
    A.price,
    dim_prices_id AS ez_prices_id,
    COALESCE(
        A.inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        A.modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    p_base A
    LEFT JOIN {{ ref('silver__asset_metadata') }}
    b_1
    ON A.currency = b_1.address
    LEFT JOIN {{ ref('silver__asset_metadata') }}
    b_2
    ON A.symbol = UPPER (
        b_2.project_name
    )
    AND A.currency IS NULL qualify(ROW_NUMBER() over(PARTITION BY recorded_hour, COALESCE(A.currency, b_1.address, b_2.address)
ORDER BY
    pro_rank) = 1)
