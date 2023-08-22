{{ config(
    materialized = 'table',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'PRICES' }} }
) }}

SELECT
    *
FROM
    {{ ref('price__dim_prices') }}
