{{ config(
    materialized = 'view'
) }}

SELECT
    *
FROM
    {{ ref('gov__fact_validators') }}
