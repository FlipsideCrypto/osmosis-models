{{ config(
    materialized = 'view',
      meta={
        'database_tags':{
            'table': {
                'PURPOSE': 'PRICES'
            }
        }
      }
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
        A.price,
        A.provider,
        CASE
            A.provider
            WHEN 'coin gecko' THEN 1
            WHEN 'coin market cap' THEN 2
            ELSE 3
        END AS pro_rank
    FROM
        {{ ref('core__dim_prices') }} A qualify(ROW_NUMBER() over(PARTITION BY recorded_hour, UPPER(symbol)
    ORDER BY
        pro_rank) = 1))
    SELECT
        A.recorded_hour,
        A.symbol,
        b.address AS currency,
        A.price
    FROM
        p_base A
        LEFT JOIN {{ ref('silver__asset_metadata') }}
        b
        ON A.symbol = UPPER (
            b.project_name
        )
