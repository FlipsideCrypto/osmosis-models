{{ config(
    materialized = 'view',
    tags = ['noncore']
) }}

SELECT
    'osmosis' AS blockchain,
    module,
    pool_created_block_timestamp,
    pool_created_block_id,
    pool_id,
    pool_address,
    assets
FROM
    {{ ref('silver__pool_metadata') }}
