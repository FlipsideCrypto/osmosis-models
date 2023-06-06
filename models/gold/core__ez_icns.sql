{{ config(
    materialized = 'view'
) }}

SELECT
    blockchain,
    creator,
    address,
    label,
    project_name,
    start_date,
    end_date
FROM
    {{ ref(
        'silver__croschain_icns'
    ) }}
