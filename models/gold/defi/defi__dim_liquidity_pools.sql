{{ config(
    materialized = 'view',
    tags = ['noncore']
) }}

SELECT
    'osmosis' AS blockchain,
    module,
    pool_created_block_timestamp,
    pool_created_block_id,
    A.pool_id,
    COALESCE(
        b.address,
        A.pool_address
    ) AS pool_address,
    COALESCE(
        b.assets,
        A.assets
    ) AS assets,
    COALESCE(
        pool_metadata_id,
        {{ dbt_utils.generate_surrogate_key(
            ['_unique_key']
        ) }}
    ) AS dim_liquidity_pools_id,
    GREATEST(
        COALESCE(
            A.inserted_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            b.inserted_timestamp,
            '2000-01-01'
        )
    ) AS inserted_timestamp,
    GREATEST(
        COALESCE(
            A.modified_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            b.modified_timestamp,
            '2000-01-01'
        )
    ) AS modified_timestamp
FROM
    {{ ref('silver__pool_metadata') }} A
    LEFT JOIN {{ ref('silver__concentrated_liquidity_pools_latest') }}
    b
    ON A.pool_id = b.pool_id
