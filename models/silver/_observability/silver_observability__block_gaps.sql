{{ config(
    materialized = 'incremental',
    full_refresh = false
) }}

WITH source AS (

    SELECT
        block_id,
        block_timestamp,
        LAG(
            block_id,
            1
        ) over (
            ORDER BY
                block_id ASC
        ) AS prev_BLOCK_ID
    FROM
        {{ ref('silver__blocks') }}

{% if is_incremental() %}
WHERE
    (
        block_timestamp :: DATE >= (
            SELECT
                MAX(
                    max_block_timestamp
                ) :: DATE -3
            FROM
                {{ this }}
        )
        OR (
            (
                SELECT
                    block_gaps
                FROM
                    {{ this }}
                    qualify(ROW_NUMBER() over(
                ORDER BY
                    max_block_timestamp DESC) = 1)
            ) <> 0
        )
    )
{% endif %}
)
SELECT
    MIN(block_id) AS min_block,
    MAX(block_id) AS max_block,
    MIN(block_timestamp) AS min_block_timestamp,
    MAX(block_timestamp) AS max_block_timestamp,
    COUNT(1) AS total_blocks,
    SUM(
        CASE
            WHEN block_id - prev_BLOCK_ID <> 1 THEN 1
            ELSE 0
        END
    ) AS block_gaps,
    ARRAY_AGG(
        CASE
            WHEN block_id - prev_BLOCK_ID <> 1 THEN OBJECT_CONSTRUCT(
                'prev_block_id',
                prev_block_id,
                'block_id',
                block_id
            )
        END
    ) AS block_gaps_details,
    SYSDATE() AS _inserted_timestamp
FROM
    source
