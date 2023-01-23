{{ config(
    materialized = 'view'
) }}

SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date
FROM
    {{ source(
        'crosschain',
        'address_tags'
    ) }}
WHERE 
    tag_type = 'ICNS'
