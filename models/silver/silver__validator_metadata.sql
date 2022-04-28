{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', operator_address, creator, blockchain)",
  incremental_strategy = 'delete+insert'
) }}

SELECT 
   operator_address AS address,
   'osmosis' AS blockchain, 
   'flipside' AS creator, 
   'operator' AS label_type, 
   'validator' AS label_subtype, 
   moniker AS label, 
   identity AS project_name, 
   value
FROM 
    {{ source(
        'osmosis_external', 
        'validator_metadata_api'
    ) }}
   