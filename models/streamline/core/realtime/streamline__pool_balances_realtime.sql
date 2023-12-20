{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_rest_api(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'pool_balances', 'sql_limit', {{var('sql_limit','1000')}}, 'producer_batch_size', {{var('producer_batch_size','200')}}, 'worker_batch_size', {{var('worker_batch_size','20')}}, 'sm_secret_name','prod/osmosis/allthatnode/mainnet-archive/rest'))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH last_block_of_hour AS (

    SELECT
        DATE_TRUNC(
            'hour',
            block_timestamp
        ) AS block_hour,
        MAX(block_id) AS block_number
    FROM
        {{ ref('silver__blocks') }}
    WHERE
        block_id >= 12791459 -- last block pulled via the old streamline
    GROUP BY
        1
),
blocks_to_call AS(
    SELECT
        block_number
    FROM
        last_block_of_hour
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref(
            'streamline__complete_pool_balances'
        ) }}
),
calls AS (
    SELECT
        '{service}/{Authentication}/osmosis/gamm/v1beta1/pools?pagination.limit=10000' AS calls,
        block_number
    FROM
        blocks_to_call
)
SELECT
    ARRAY_CONSTRUCT(
        block_number,
        ARRAY_CONSTRUCT(
            'GET',
            calls,
            OBJECT_CONSTRUCT(
                'x-cosmos-block-height',
                block_number :: STRING
            ),
            PARSE_JSON('{}'),
            ''
        )
    ) AS request
FROM
    calls
ORDER BY
    block_number
