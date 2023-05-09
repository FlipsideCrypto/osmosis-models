{{ config (
    materialized = 'view'
) }}

SELECT
    *
FROM
    {{ source(
        'bronze_streamline',
        'pool_balances_api'
    ) }}
