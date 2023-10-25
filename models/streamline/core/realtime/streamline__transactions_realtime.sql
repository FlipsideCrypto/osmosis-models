{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_rest_api(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'transactions', 'sql_limit', {{var('sql_limit','10000')}}, 'producer_batch_size', {{var('producer_batch_size','200')}}, 'worker_batch_size', {{var('worker_batch_size','20')}}, 'exploded_key', '[\"txs;tx_responses\"]'))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH blocks AS (

    SELECT
        block_number
    FROM
        {{ ref("streamline__complete_txcount") }}
    WHERE
        block_number > 11800000
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref("streamline__complete_transactions") }}
),
transactions_counts_by_block AS (
    SELECT
        tc.block_number,
        tc.data :: INTEGER AS txcount
    FROM
        {{ ref("bronze__streamline_FR_txcount") }}
        tc
        INNER JOIN blocks b
        ON tc.block_number = b.block_number
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
                ROUND((n.n -1) * 100) :: STRING AS pagination_offset
            FROM
                transactions_counts_by_block tt
                JOIN numbers n
                ON n.n <= CASE
                    WHEN tt.txcount % 100 = 0 THEN tt.txcount / 100
                    ELSE FLOOR(
                        tt.txcount / 100
                    ) + 1
                END
        ),
        blocks_with_page_numbers_to_read AS (
            SELECT
                block_number,
                pagination_offset
            FROM
                blocks_with_page_numbers
            EXCEPT
            SELECT
                block_number,
                pagination_offset
            FROM
                {{ ref("streamline__complete_transactions") }}
        )
    SELECT
        block_number,
        ARRAY_CONSTRUCT(
            'GET',
            '/cosmos/tx/v1beta1/txs',
            PARSE_JSON('{}'),
            PARSE_JSON(
                CONCAT(
                    '{"params":{',
                    '"events":"tx.height=',
                    block_number :: STRING,
                    '",',
                    '"pagination.limit":"100"',
                    ',',
                    '"pagination.offset":"',
                    pagination_offset,
                    '"},"id":"',
                    block_number :: STRING,
                    '"}'
                )
            ),
            ''
        ) AS request
    FROM
        blocks_with_page_numbers_to_read
    ORDER BY
        block_number DESC
