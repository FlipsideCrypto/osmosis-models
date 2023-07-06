{{ config(
    materialized = 'incremental',
    full_refresh = false
) }}

WITH max_silver AS (

    SELECT
        MAX(block_id) AS max_block_id
    FROM
        {{ ref('silver__transactions') }}
),
bronze AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id
    FROM
        {{ ref('bronze__transactions') }}
    WHERE
        block_id <= (
            SELECT
                max_block_id
            FROM
                max_silver
        )

{% if is_incremental() %}
AND (
    _inserted_timestamp >= CURRENT_DATE - 7
    AND block_timestamp :: DATE >= (
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
                blocks_missing_transactions
            FROM
                {{ this }}
                qualify(ROW_NUMBER() over(
            ORDER BY
                max_block_timestamp DESC) = 1)
        ) <> 0
    )
)
{% endif %}

qualify(DENSE_RANK() over(PARTITION BY block_id
ORDER BY
    _inserted_timestamp DESC) = 1)
),
bronze_count AS (
    SELECT
        block_id,
        block_timestamp,
        COUNT(
            DISTINCT tx_id
        ) AS num_txs
    FROM
        bronze
    GROUP BY
        block_id,
        block_timestamp
),
bronze_api AS (
    SELECT
        block_id,
        block_timestamp,
        num_txs
    FROM
        {{ ref('silver__blockchain') }}
    WHERE
        block_id <= (
            SELECT
                max_block_id
            FROM
                max_silver
        )

{% if is_incremental() %}
AND (
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
                blocks_missing_transactions
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
    MIN(
        A.block_id
    ) AS min_block,
    MAX(
        A.block_id
    ) AS max_block,
    MIN(
        A.block_timestamp
    ) AS min_block_timestamp,
    MAX(
        A.block_timestamp
    ) AS max_block_timestamp,
    COUNT(1) AS total_blocks,
    SUM(
        CASE
            WHEN COALESCE(
                b.num_txs,
                0
            ) - A.num_txs <> 0 THEN 1
            ELSE 0
        END
    ) AS blocks_missing_transactions,
    SUM(
        ABS(
            COALESCE(
                b.num_txs,
                0
            ) - A.num_txs
        )
    ) AS total_missing_transactions,
    ARRAY_AGG(
        CASE
            WHEN COALESCE(
                b.num_txs,
                0
            ) - A.num_txs <> 0 THEN OBJECT_CONSTRUCT(
                'block',
                A.block_id,
                'block_timestamp',
                A.block_timestamp,
                'diff',
                COALESCE(
                    b.num_txs,
                    0
                ) - A.num_txs,
                'blockchain_num_txs',
                A.num_txs,
                'bronze_num_txs',
                COALESCE(
                    b.num_txs,
                    0
                )
            )
        END
    ) within GROUP(
        ORDER BY
            A.block_id
    ) AS missing_transactions_details,
    SYSDATE() AS _inserted_timestamp {# SELECT
    A.block_id AS min_block,
    A.block_id AS max_block,
    A.block_timestamp AS min_block_timestamp,
    A.block_timestamp AS max_block_timestamp,
    1 AS total_blocks,
    CASE
        WHEN A.num_txs - b.num_txs <> 0 THEN 1
        ELSE 0
    END AS blocks_missing_transactions,
    CASE
        WHEN A.num_txs - b.num_txs <> 0 THEN OBJECT_CONSTRUCT(
            'block_id',
            A.block_id,
            'diff',
            A.num_txs - b.num_txs,
            'blockchain_num_txs',
            b.num_txs,
            'bronze_num_txs',
            A.num_txs
        )
    END AS missing_transactions_details,
    SYSDATE() AS _inserted_timestamp #}
FROM
    bronze_api A
    LEFT JOIN bronze_count b
    ON A.block_id = b.block_id
