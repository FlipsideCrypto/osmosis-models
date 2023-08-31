{{ config(
    materialized = 'view'
) }}

SELECT
    *
FROM
    {{ ref('gov__dim_liquidity_pools') }}
