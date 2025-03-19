{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"pool_balances_v2",
        "sql_limit" :"2000",
        "producer_batch_size" :"10",
        "worker_batch_size" :"10",
        "exploded_key": "[\"pools\"]",
        "sql_source" :"{{this.identifier}}" }
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
        block_id >= 12791459 -- last block pulled via the v1 streamline
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
        {{ ref('streamline__complete_pool_balances') }}
)
SELECT
    ROUND(
        block_number,
        -3
    ) AS partition_key,
    {{ target.database }}.live.udf_api(
        'GET',
        REPLACE(
            'https://osmosis-api.lavenderfive.com/osmosis/gamm/v1beta1/pools?pagination.limit=10000',
            'tendermint',
            'rest'
        ),
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json',
            'x-cosmos-block-height',
            block_number :: STRING
        ),
        PARSE_JSON('{}')
    ) AS request,
    block_number
FROM
    blocks_to_call
ORDER BY
    block_number DESC
