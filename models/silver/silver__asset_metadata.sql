{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', address, creator)",
  incremental_strategy = 'delete+insert'
) }}

SELECT 
    'osmosis' AS blockchain, 
    base AS address, 
    'flipside' AS creator, 
    'token' AS label_type, 
    'token_contract' AS label_subtype, 
    name AS label, 
    symbol AS project_name, 
    description, 
    denom_units AS value
FROM {{ source(
        'osmosis_external', 
        'asset_metadata_api'
    ) }}

-- How do we do incremental on something with no inserted_at timestamp? 