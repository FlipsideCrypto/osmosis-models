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
bronze_count AS (
    SELECT
        block_id,
        MIN(block_timestamp) AS block_timestamp,
        COUNT(1) num_txs
    FROM
        (
            SELECT
                block_id,
                block_timestamp,
                tx_id
            FROM
                bronze A qualify(ROW_NUMBER() over(PARTITION BY tx_id
            ORDER BY
                block_id DESC, _inserted_timestamp DESC) = 1)
        )
    GROUP BY
        block_id
),
silver AS (
    SELECT
        block_id,
        MIN(block_timestamp) AS block_timestamp,
        COUNT(1) AS num_txs
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        block_timestamp BETWEEN (
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
    GROUP BY
        block_id
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
                'bronze_num_txs',
                A.num_txs,
                'silver_num_txs',
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
    bronze_count A
    LEFT JOIN silver b
    ON A.block_id = b.block_id
