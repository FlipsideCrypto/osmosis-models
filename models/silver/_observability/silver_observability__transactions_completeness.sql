{{ config(
    materialized = 'incremental',
    full_refresh = false
) }}

WITH rel_blocks AS (

    SELECT
        block_id,
        block_timestamp
    FROM
        {{ ref('silver__blocks') }}
    WHERE
        block_timestamp < DATEADD(
            HOUR,
            -24,
            SYSDATE()
        )

{% if is_incremental() %}
AND (
    block_timestamp >= DATEADD(
        HOUR,
        -96,(
            SELECT
                MAX(
                    max_block_timestamp
                )
            FROM
                {{ this }}
        )
    )
    OR ({% if var('OBSERV_FULL_TEST') %}
        block_id >= 0
    {% else %}
        block_id >= (
    SELECT
        MIN(VALUE) - 1
    FROM
        (
    SELECT
        blocks_impacted_array
    FROM
        {{ this }}
        qualify ROW_NUMBER() over (
    ORDER BY
        test_timestamp DESC) = 1), LATERAL FLATTEN(input => blocks_impacted_array))
    {% endif %})
)
{% endif %}
),
bronze AS (
    SELECT
        A.block_id,
        A.block_id_requested,
        b.block_timestamp,
        A.tx_id,
        A._inserted_timestamp
    FROM
        {{ ref('bronze__transactions_2') }} A
        LEFT JOIN rel_blocks b
        ON A.block_id = b.block_id
        LEFT JOIN rel_blocks C
        ON A.block_id_requested = C.block_id
    WHERE
        (
            b.block_id IS NOT NULL
            OR C.block_id IS NOT NULL
        )
    UNION ALL
    SELECT
        block_id,
        block_id,
        block_timestamp,
        tx_id,
        _inserted_timestamp
    FROM
        {{ source(
            'bronze',
            'chainwalkers_txs'
        ) }}
    UNION ALL
    SELECT
        block_id,
        block_id,
        block_timestamp,
        tx_id,
        _inserted_timestamp
    FROM
        {{ ref('silver___manual_tx_lq') }}
),
b_block AS (
    SELECT
        A.block_id,
        A.block_id_requested,
        A.block_timestamp,
        A.tx_id,
        A._inserted_timestamp
    FROM
        bronze A qualify(ROW_NUMBER() over(PARTITION BY A.block_id, tx_id
    ORDER BY
        A._inserted_timestamp DESC) = 1)
),
b_block_req AS (
    SELECT
        A.block_id,
        A.block_id_requested,
        A.block_timestamp,
        A.tx_id,
        A._inserted_timestamp
    FROM
        bronze A qualify(ROW_NUMBER() over(PARTITION BY A.block_id_requested, tx_id
    ORDER BY
        A._inserted_timestamp DESC) = 1)
),
bronze_count AS (
    SELECT
        block_id,
        MIN(block_timestamp) AS block_timestamp,
        MAX(num_txs) num_txs
    FROM
        (
            SELECT
                block_id,
                block_timestamp,
                COUNT(
                    DISTINCT tx_id
                ) AS num_txs
            FROM
                b_block A
            GROUP BY
                block_id,
                block_timestamp
            UNION ALL
            SELECT
                block_id_requested AS block_id,
                MIN(block_timestamp) AS block_timestamp,
                COUNT(
                    DISTINCT tx_id
                ) AS num_txs
            FROM
                b_block_req A
            GROUP BY
                block_id_requested
        )
    GROUP BY
        block_id
),
bronze_api AS (
    SELECT
        A.block_id,
        A.block_timestamp,
        LEAST(
            A.num_txs,
            b.num_txs
        ) AS num_txs
    FROM
        {{ ref('silver__blockchain') }} A
        LEFT JOIN {{ ref('bronze__tx_count') }}
        b
        ON A.block_id = b.block_id
    WHERE
        A.block_timestamp BETWEEN (
            SELECT
                MIN(block_timestamp)
            FROM
                rel_blocks
        )
        AND (
            SELECT
                MAX(block_timestamp)
            FROM
                rel_blocks
        )
        AND COALESCE(
            b.num_txs,
            0
        ) > 0
        AND A.block_id NOT IN (
            12028887,
            12028888
        )
)
SELECT
    'transactions' AS test_name,
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
    COUNT(1) AS blocks_tested,
    SUM(
        CASE
            WHEN COALESCE(
                b.num_txs,
                0
            ) - A.num_txs <> 0 THEN 1
            ELSE 0
        END
    ) AS blocks_impacted_count,
    ARRAY_AGG(
        CASE
            WHEN COALESCE(
                b.num_txs,
                0
            ) - A.num_txs <> 0 THEN A.block_id
        END
    ) within GROUP (
        ORDER BY
            A.block_id
    ) AS blocks_impacted_array,
    SUM(
        ABS(
            COALESCE(
                b.num_txs,
                0
            ) - A.num_txs
        )
    ) AS transactions_impacted_count,
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
    ) AS test_failure_details,
    SYSDATE() AS test_timestamp
FROM
    bronze_api A
    LEFT JOIN bronze_count b
    ON A.block_id = b.block_id
