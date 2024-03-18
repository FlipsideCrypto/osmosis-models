{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_rest_api(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'txcount', 'sql_limit', {{var('sql_limit','2000000')}}, 'producer_batch_size', {{var('producer_batch_size','1000')}}, 'worker_batch_size', {{var('worker_batch_size','100')}}, 'batch_call_limit', {{var('batch_call_limit','10')}}, 'exploded_key', '[\"result\", \"total_count\"]', 'call_type', 'batch', 'sm_secret_name','prod/osmosis/allthatnode/mainnet-archive/rpc'))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_core_realtime']
) }}

WITH blocks AS (

    SELECT
        block_number
    FROM
        {{ ref("streamline__blocks") }}
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref("streamline__complete_txcount") }}
)
SELECT
    block_number AS partition_key,
    OBJECT_CONSTRUCT(
        'method',
        'POST',
        'url',
        '{service}/{Authentication}',
        'headers',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json'
        ),
        'params',
        PARSE_JSON('{}'),
        'data',
        OBJECT_CONSTRUCT(
            'id',
            block_number :: STRING,
            'jsonrpc',
            '2.0',
            'method',
            'tx_search',
            'params',
            ARRAY_CONSTRUCT(
                'tx.height=' || block_number::STRING,
                TRUE,
                '1',
                '1',
                'asc'
            )
        ) :: STRING
    ) AS request
FROM
    blocks
ORDER BY
    partition_key ASC
