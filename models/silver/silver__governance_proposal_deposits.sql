{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE'],
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

proposal_ids AS (
    SELECT
        tx_id,
        attribute_value AS proposal_id
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        msg_type = 'proposal_deposit'
        AND attribute_key = 'proposal_id'

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
deposit_value AS (
    SELECT
        tx_id,
        SPLIT_PART(
            TRIM(
                REGEXP_REPLACE(
                    attribute_value,
                    '[^[:digit:]]',
                    ' '
                )
            ),
            ' ',
            0
        ) / pow(10, COALESCE(raw_metadata [1] :exponent, 0)) AS amount,
        RIGHT(attribute_value, LENGTH(attribute_value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))) AS currency,
        raw_metadata [1] :exponent AS DECIMAL
    FROM
        {{ ref('silver__msg_attributes') }}
        m
        LEFT OUTER JOIN {{ ref('silver__asset_metadata') }} A
        ON RIGHT(attribute_value, LENGTH(attribute_value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))) = A.address
    WHERE
        msg_type = 'proposal_deposit'
        AND attribute_key = 'amount'
        AND attribute_value IS NOT NULL

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
depositors AS (
    SELECT
        tx_id,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS depositor
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        attribute_key = 'acc_seq'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_id,
    block_timestamp,
    blockchain,
    chain_id,
    p.tx_id,
    tx_status,
    d.depositor,
    p.proposal_id,
    v.amount,
    v.currency,
    DECIMAL,
    _inserted_timestamp
FROM
    deposit_value v
    INNER JOIN proposal_ids p
    ON p.tx_id = v.tx_id
    INNER JOIN depositors d
    ON v.tx_id = d.tx_id
    LEFT OUTER JOIN {{ ref('silver__transactions') }}
    t
    ON v.tx_id = t.tx_id

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
