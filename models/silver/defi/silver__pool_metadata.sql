{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
) }}

WITH

{% if is_incremental() %}
max_date AS (

    SELECT
        MAX(
            _inserted_timestamp
        ) _inserted_timestamp
    FROM
        {{ this }}
),
{% endif %}

pool_creation_txs AS (
    SELECT
        DISTINCT tx_id,
        block_timestamp,
        block_id
    FROM
        {{ ref('silver__msgs') }}
    WHERE
        msg_type = 'pool_created'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        max_date
)
{% endif %}
),
b AS (
    SELECT
        ma.tx_id,
        msg_type,
        msg_index,
        attribute_index,
        attribute_key,
        attribute_value,
        _inserted_timestamp
    FROM
        {{ ref('silver__msg_attributes') }}
        ma
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

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        max_date
)
{% endif %}
),
C AS (
    SELECT
        tx_id,
        _inserted_timestamp,
        OBJECT_AGG(
            attribute_key,
            attribute_value :: variant
        ) AS obj
    FROM
        (
            SELECT
                DISTINCT tx_id,
                attribute_key,
                attribute_value,
                _inserted_timestamp
            FROM
                b
        )
    GROUP BY
        tx_id,
        _inserted_timestamp
),
d AS (
    SELECT
        tx_id,
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
        TABLE(FLATTEN(SPLIT(obj :amount, ','))) A
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