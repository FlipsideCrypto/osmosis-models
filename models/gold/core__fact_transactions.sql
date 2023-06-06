{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH fee AS (

    SELECT
        tx_id,
        attribute_value AS fee
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        attribute_key = 'fee'

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= CURRENT_DATE -2
{% endif %}
qualify(ROW_NUMBER() over(PARTITION BY tx_id
ORDER BY
    msg_index)) = 1
),
spender AS (
    SELECT
        tx_id,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS tx_from
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        attribute_key = 'acc_seq'

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= CURRENT_DATE -2
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY tx_id
ORDER BY
    msg_index)) = 1
)
SELECT
    t.block_id,
    t.block_timestamp,
    t.tx_id,
    s.tx_from,
    tx_succeeded,
    codespace,
    COALESCE(
        fee,
        '0uosmo'
    ) AS fee,
    gas_used,
    gas_wanted,
    tx_code,
    msgs
FROM
    {{ ref('silver__transactions') }}
    t
    LEFT OUTER JOIN fee f
    ON t.tx_id = f.tx_id
    LEFT OUTER JOIN spender s
    ON t.tx_id = s.tx_id

{% if is_incremental() %}
WHERE
    _inserted_timestamp :: DATE >= CURRENT_DATE -2
{% endif %}
