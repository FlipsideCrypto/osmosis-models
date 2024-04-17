{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_rest_api(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'tx_search', 'sql_limit', {{var('sql_limit','20000')}}, 'producer_batch_size', {{var('producer_batch_size','200')}}, 'worker_batch_size', {{var('worker_batch_size','10')}}, 'batch_call_limit', {{var('batch_call_limit','10')}}, 'exploded_key', '[\"result\", \"txs\"]', 'call_type', 'non_batch', 'sm_secret_name','prod/osmosis/allthatnode/mainnet-archive/rpc'))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_core_realtime']
) }}
-- depends_on: {{ ref('streamline__complete_tx_search') }}
WITH transactions_counts_by_block AS (

    SELECT
        tc.block_number,
        tc.txcount
    FROM
        {{ ref("streamline__complete_txcount") }}
        tc
),
numbers AS (
    -- Recursive CTE to generate numbers. We'll use the maximum txcount value to limit our recursion.
    SELECT
        1 AS n
    UNION ALL
    SELECT
        n + 1
    FROM
        numbers
    WHERE
        n < (
            SELECT
                CEIL(MAX(txcount) / 100.0)
            FROM
                transactions_counts_by_block)
        ),
        blocks_with_page_numbers AS (
            SELECT
                tt.block_number AS block_number,
                n.n AS page_number
            FROM
                transactions_counts_by_block tt
                JOIN numbers n
                ON n.n <= CASE
                    WHEN tt.txcount % 100 = 0 THEN tt.txcount / 100
                    ELSE FLOOR(
                        tt.txcount / 100
                    ) + 1
                END

{% if is_incremental() %}
EXCEPT
SELECT
    block_number,
    page
FROM
    {{ ref("streamline__complete_tx_search") }}
{% endif %}
)
SELECT
    ROUND(
        block_number,
        -3
    ) AS partition_key,
    {{ target.database }}.live.udf_api(
        'POST',
        '{service}/{Authentication}',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json'
        ),
        OBJECT_CONSTRUCT(
            'id',
            block_number,
            'jsonrpc',
            '2.0',
            'method',
            'tx_search',
            'params',
            ARRAY_CONSTRUCT(
                'tx.height=' || block_number :: STRING,
                TRUE,
                page_number :: STRING,
                '100',
                'asc'
            )
        ),
        'vault/prod/osmosis/allthatnode/mainnet-archive/rpc'
    ) AS request
FROM
    blocks_with_page_numbers
ORDER BY
    partition_key ASC