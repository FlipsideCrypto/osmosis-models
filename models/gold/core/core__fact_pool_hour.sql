{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEFI, PRICES' }} }
) }}

SELECT
    *
FROM
    {{ ref('defi__fact_pool_hour') }}
