{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', account_address, creator, blockchain)",
  incremental_strategy = 'delete+insert'
) }}

SELECT 
   account_address,
   operator_address,
   consensus_pubkey, 
   'osmosis' AS blockchain, 
   'flipside' AS creator, 
   'operator' AS label_type, 
   'validator' AS label_subtype, 
   moniker AS label, 
   identity AS project_name, 
   details AS description,
   value
FROM 
    {{ source(
        'osmosis_external', 
        'validator_metadata_api'
    ) }}
   