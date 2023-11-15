{{ config(
    materialized = 'view',
    tags = ['noncore']
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
