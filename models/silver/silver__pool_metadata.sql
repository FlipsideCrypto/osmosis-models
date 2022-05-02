{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', module, pool_id)",
    incremental_strategy = 'delete+insert',
) }}

WITH pool_creation_txs AS (

    SELECT
        DISTINCT tx_id
    FROM
        {{ ref('silver__msgs') }}
    WHERE
        msg_type = 'pool_created'

{% if is_incremental() %}
AND ingested_at :: DATE <= CURRENT_DATE - 2
{% endif %}
),
b AS (
    SELECT
        ma.tx_id,
        msg_type,
        msg_index,
        attribute_index,
        attribute_key,
        attribute_value
    FROM
        {{ ref('silver__msg_attributes') }}
        ma
        INNER JOIN pool_creation_txs t
        ON t.tx_id = ma.tx_id
    WHERE
        (attribute_key IN ('module', 'pool_id')
        OR msg_type = 'transfer'
        AND attribute_key = 'amount'
        AND attribute_value IS NOT NULL
        AND attribute_value NOT LIKE '%/pool/%'
        AND ARRAY_SIZE(SPLIT(attribute_value, ',')) :: NUMBER > 1)

{% if is_incremental() %}
AND ingested_at :: DATE <= CURRENT_DATE - 2
{% endif %}
),
C AS (
    SELECT
        tx_id,
        OBJECT_AGG(
            attribute_key,
            attribute_value :: variant
        ) AS obj
    FROM
        b
    GROUP BY
        1
),
d AS (
    SELECT
        tx_id,
        obj :module :: STRING AS module,
        obj :pool_id :: NUMBER AS pool_id,
        'asset_address' AS object_key,
        LTRIM(
            A.value,
            '0123456789'
        ) AS asset_address
    FROM
        C,
        TABLE(FLATTEN(SPLIT(obj :amount, ','))) A
),
e AS (
    SELECT
        tx_id,
        module,
        pool_id,
        asset_address,
        OBJECT_AGG(
            object_key,
            asset_address :: variant
        ) AS asset_obj
    FROM
        d
    GROUP BY
        1,
        2,
        3,
        4
)
SELECT
    module,
    pool_id,
    ARRAY_AGG(asset_obj) AS assets
FROM
    e
GROUP BY
    1,
    2
