{{ config (
    materialized = 'view'
) }}

SELECT
    *
FROM
    {{ source(
        'bronze_streamline',
        'balances_api'
    ) }}
