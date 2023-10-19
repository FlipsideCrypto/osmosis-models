{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
) }}

WITH base_atts AS (

    SELECT
        tx_id,
        block_timestamp,
        block_id,
        msg_type,
        msg_index,
        attribute_index,
        attribute_key,
        attribute_value,
        _inserted_timestamp
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        msg_type IN (
            'pool_created',
            'transfer',
            'message'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) _inserted_timestamp
    FROM
        {{ this }}
)
{% endif %}
),
pool_creation_txs AS (
    SELECT
        tx_id,
        block_timestamp,
        block_id,
        COUNT(
            DISTINCT attribute_value
        ) pool_count
    FROM
        base_atts
    WHERE
        msg_type = 'pool_created'
        AND attribute_key = 'pool_id'
    GROUP BY
        tx_id,
        block_timestamp,
        block_id
),
b AS (
    SELECT
        ma.tx_id,
        msg_type,
        msg_index,
        attribute_index,
        attribute_key,
        attribute_value,
        CASE
            WHEN pool_count = 1 THEN 0
            ELSE NULLIF(
                (
                    conditional_true_event(
                        CASE
                            WHEN ma.msg_type = 'pool_created' THEN TRUE
                            ELSE FALSE
                        END
                    ) over (
                        PARTITION BY ma.tx_id
                        ORDER BY
                            ma.msg_index
                    ) -1
                ),
                -1
            )
        END AS pool_group,
        _inserted_timestamp
    FROM
        base_atts ma
        INNER JOIN pool_creation_txs t
        ON t.tx_id = ma.tx_id
    WHERE
        (
            attribute_key IN (
                'module',
                'pool_id'
            )
            OR (
                msg_type = 'transfer'
                AND attribute_key = 'amount'
                AND attribute_value IS NOT NULL
                AND ARRAY_SIZE(SPLIT(attribute_value, ',')) :: NUMBER > 1
            )
            OR (
                msg_type = 'transfer'
                AND len(attribute_value) = 63
            )
        )
        AND attribute_value <> 'poolmanager'
),
bb AS (
    SELECT
        tx_id,
        msg_index,
        pool_group,
        msg_type,
        _inserted_timestamp,
        attribute_key,
        attribute_value
    FROM
        b qualify(ROW_NUMBER() over (PARTITION BY tx_id, pool_group, msg_type, attribute_key
    ORDER BY
        msg_index) = 1)
),
b_plus AS (
    SELECT
        tx_id,
        msg_index,
        pool_group,
        msg_type,
        _inserted_timestamp,
        OBJECT_AGG(
            attribute_key,
            attribute_value :: variant
        ) j
    FROM
        bb
    GROUP BY
        tx_id,
        msg_index,
        pool_group,
        msg_type,
        _inserted_timestamp
),
C AS (
    SELECT
        tx_id,
        pool_group,
        _inserted_timestamp,
        OBJECT_AGG(
            attribute_key,
            attribute_value :: variant
        ) AS obj
    FROM
        b
    WHERE
        tx_id || msg_index IN (
            SELECT
                tx_id || msg_index
            FROM
                b_plus
            WHERE
                msg_type = 'pool_created'
                OR (
                    msg_type = 'transfer'
                    AND j :amount IS NOT NULL
                    AND j :amount LIKE '%,%'
                )
        )
    GROUP BY
        tx_id,
        pool_group,
        _inserted_timestamp
),
d AS (
    SELECT
        tx_id,
        pool_group,
        COALESCE(
            obj :module :: STRING,
            'gamm'
        ) AS module,
        obj :pool_id :: NUMBER AS pool_id,
        obj :recipient :: STRING AS pool_address,
        'asset_address' AS object_key,
        LTRIM(
            A.value,
            '0123456789'
        ) AS asset_address,
        _inserted_timestamp
    FROM
        C,
        TABLE(FLATTEN(SPLIT(obj :amount, ','), outer => TRUE)) A
),
e AS (
    SELECT
        tx_id,
        module,
        pool_id,
        pool_address,
        asset_address,
        OBJECT_AGG(
            object_key,
            asset_address :: variant
        ) AS asset_obj,
        _inserted_timestamp
    FROM
        d
    GROUP BY
        tx_id,
        module,
        pool_id,
        pool_address,
        asset_address,
        _inserted_timestamp
)
SELECT
    module,
    C.block_timestamp AS pool_created_block_timestamp,
    C.block_id AS pool_created_block_id,
    pool_id,
    pool_address,
    ARRAY_AGG(asset_obj) AS assets,
    concat_ws(
        '-',
        module,
        pool_id
    ) AS _unique_key,
    _inserted_timestamp
FROM
    e
    JOIN pool_creation_txs C
    ON C.tx_id = e.tx_id
GROUP BY
    module,
    C.block_timestamp,
    C.block_id,
    pool_id,
    pool_address,
    _inserted_timestamp
