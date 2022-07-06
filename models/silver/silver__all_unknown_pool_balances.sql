{{ config(
    materialized = 'view',
    post_hook = 'call silver.sp_bulk_get_pool_balances()',
) }}

WITH last_block_of_date AS (

    SELECT
        block_timestamp :: DATE,
        MAX(block_id) AS block_id
    FROM
        {{ ref('silver__blocks') }}
    WHERE
        block_id >= 2300000
        AND block_timestamp :: DATE <= CURRENT_DATE - 1
    GROUP BY
        1
)
SELECT
    block_id
FROM
    last_block_of_date
UNION
SELECT
    2300000 -- origin block of data available from api
EXCEPT
SELECT
    block_id
FROM
    {{ source(
        'osmosis_external',
        'pool_balances_api'
    ) }}
LIMIT
    1000
