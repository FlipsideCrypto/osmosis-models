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
    assets,
    COALESCE(
        pool_metadata_id,
        {{ dbt_utils.generate_surrogate_key(
            ['_unique_key']
        ) }}
    ) AS dim_liquidity_pools_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__pool_metadata') }}
