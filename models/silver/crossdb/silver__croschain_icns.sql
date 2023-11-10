{{ config(
    materialized = 'table',
    tags = ['daily']
) }}

SELECT
    blockchain,
    creator,
    address,
    tag_name AS label,
    tag_type AS project_name,
    start_date,
    end_date
FROM
    {{ source(
        'crosschain',
        'dim_tags'
    ) }}
WHERE
    tag_type = 'ICNS'
