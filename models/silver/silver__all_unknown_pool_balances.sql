{{ config(
    materialized = 'view',
    post_hook = 'call silver.sp_bulk_get_pool_balances()',
) }}

WITH last_block_of_hour AS (

    SELECT
        DATE_TRUNC(
            'hour',
            block_timestamp
        ) AS block_hour,
        MAX(block_id) AS block_id
    FROM
        {{ ref('silver__blocks') }}
    WHERE
        block_id >= 2300000 {# AND block_timestamp :: DATE <= CURRENT_DATE - 1 #}
    GROUP BY
        1
)
SELECT
    block_id
FROM
    last_block_of_hour
UNION
SELECT
    2300000 -- origin block of data available from api
EXCEPT
SELECT
    block_id
FROM
    {{ source(
        'bronze_streamline',
        'pool_balances_api'
    ) }}
ORDER BY
    block_id DESC
