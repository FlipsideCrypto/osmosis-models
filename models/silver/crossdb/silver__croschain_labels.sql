{{ config(
    materialized = 'table'
) }}

SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    label_type,
    label_subtype,
    address_name,
    project_name
FROM
    {{ source(
        'crosschain',
        'dim_address_labels'
    ) }}
WHERE
    blockchain = 'osmosis'
