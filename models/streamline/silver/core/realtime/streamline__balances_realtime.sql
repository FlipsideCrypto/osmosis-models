{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"balances_v2",
        "sql_limit" :"10000",
        "producer_batch_size" :"40",
        "worker_batch_size" :"40",
        "sql_source" :"{{this.identifier}}" }
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
)
SELECT
    ROUND(
        block_number,
        -3
    ) AS partition_key,
    {{ target.database }}.live.udf_api(
        'GET',
        'https://osmosis-api.lavenderfive.com/cosmos/bank/v1beta1/balances/' || address || '?pagination.limit=1000',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json',
            'x-cosmos-block-height',
            block_number :: STRING
        ),
        PARSE_JSON('{}')
    ) AS request,
    block_number,
    address
FROM
    blocks_to_call
ORDER BY
    block_number DESC
LIMIT
    10000
