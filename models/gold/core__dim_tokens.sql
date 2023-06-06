{{ config(
    materialized = 'view'
) }}

SELECT 
    blockchain, 
    address, 
    creator, 
    label_type, 
    label_subtype, 
    label, 
    project_name, 
    alias, 
    decimal, 
    raw_metadata
FROM {{ ref('silver__asset_metadata') }}