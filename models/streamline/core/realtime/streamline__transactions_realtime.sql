{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_rest_api(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'transactions', 'sql_limit', {{var('sql_limit','10000')}}, 'producer_batch_size', {{var('producer_batch_size','200')}}, 'worker_batch_size', {{var('worker_batch_size','20')}}, 'exploded_key', '[\"txs;tx_responses\"]', 'sm_secret_name','prod/osmosis/allthatnode/mainnet-archive/rest'))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH blocks AS (

    SELECT
        A.block_number,
        A.txcount
    FROM
        {{ ref("streamline__complete_txcount") }} A
        LEFT JOIN {{ ref("streamline__complete_transactions") }}
        b
        ON A.block_number = b.block_number
    WHERE
        A.block_number = 12206934
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
                blocks)
        ),
        blocks_with_page_numbers AS (
            SELECT
                tt.block_number AS block_number,
                ROUND((n.n -1) * 100) :: STRING AS pagination_offset
            FROM
                blocks tt
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
        ),
        calls AS (
            SELECT
                '{service}/{Authentication}/cosmos/tx/v1beta1/txs?events=tx.height%3D' || block_number :: STRING || '&pagination.limit=100&pagination.offset=' || pagination_offset :: STRING AS calls,
                block_number
            FROM
                blocks_with_page_numbers_to_read
        )
    SELECT
        ARRAY_CONSTRUCT(
            block_number,
            ARRAY_CONSTRUCT(
                'GET',
                calls,
                PARSE_JSON('{}'),
                PARSE_JSON('{}'),
                ''
            )
        ) AS request
    FROM
        calls
    ORDER BY
        block_number
