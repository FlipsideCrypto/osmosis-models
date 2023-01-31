{{ config(
    materialized = 'view'
) }}

SELECT
    blockchain,
    creator,
    address,
    tag_name as label,
    tag_type as project_name,
    start_date,
    end_date
FROM
    {{ source(
        'crosschain',
        'address_tags'
    ) }}
WHERE 
    tag_type = 'ICNS'
