{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_rest_api(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'balances', 'sql_limit', {{var('sql_limit','1000000')}}, 'producer_batch_size', {{var('producer_batch_size','1000')}}, 'worker_batch_size', {{var('worker_batch_size','100')}}, 'sm_secret_name','prod/osmosis/allthatnode/mainnet-archive/rest'))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH blocks_to_call AS(

    SELECT
        block_id AS block_number,
        address
    FROM
        {{ ref('streamline__balances') }}
    EXCEPT
    SELECT
        block_number,
        address
    FROM
        {{ ref('streamline__complete_balances') }}
),
calls AS (
    SELECT
        '{service}/{Authentication}/cosmos/bank/v1beta1/balances/' || address || '?pagination.limit=10000' AS calls,
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
