{{ config(
    materialized = 'view'
) }}

SELECT
    *
FROM
    {{ ref('defi__fact_liquidity_provider_actions') }}