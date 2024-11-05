{{ config (
    materialized = 'view',
    enabled = false
) }}

SELECT
    *
FROM
    {{ source(
        'bronze_streamline',
        'balances_api'
    ) }}
