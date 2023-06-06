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
    raw_metadata
FROM 
    {{ ref('silver__asset_metadata') }}

UNION ALL 

SELECT
    blockchain, 
    address, 
    creator, 
    label_type, 
    label_subtype, 
    label, 
    project_name, 
    raw_metadata
FROM 
    {{ ref('silver__validator_metadata') }}
