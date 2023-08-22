{{ config(
    materialized = 'view'
) }}

SELECT
    *
FROM
    {{ ref('defi__fact_locked_liquidity_actions') }}
