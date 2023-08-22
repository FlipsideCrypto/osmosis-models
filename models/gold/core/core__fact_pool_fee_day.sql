{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEFI' }} }
) }}

SELECT
    *
FROM
    {{ ref('defi__fact_pool_fee_day') }}
