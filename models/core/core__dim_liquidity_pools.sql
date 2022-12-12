{{ config(
    materialized = 'view'
) }}

SELECT 
    'osmosis' AS blockchain, 
    module,
    pool_id, 
    assets
FROM {{ ref('silver__pool_metadata') }}