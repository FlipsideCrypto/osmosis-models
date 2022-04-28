{{ config(
  materialized = 'table'
) }}

SELECT 
    'osmosis' AS blockchain, 
    base AS address, 
    'flipside' AS creator, 
    'token' AS label_type, 
    'token_contract' AS label_subtype, 
    name AS label, 
    symbol AS project_name,  
    denom_units AS value
FROM {{ source(
        'osmosis_external', 
        'asset_metadata_api'
    ) }}