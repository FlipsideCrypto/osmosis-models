{{ config(
    materialized = 'incremental',
    unique_key = ["tx_id","msg_index"],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['noncore']
) }}

WITH bhour AS (

    SELECT
        DATE_TRUNC(
            'hour',
            block_timestamp
        ) AS block_hour,
        MAX(block_id) AS block_id_hour
    FROM
        {{ ref('silver__blocks') }}
    WHERE
        block_id >= 2300000 {# AND block_timestamp :: DATE <= CURRENT_DATE - 1 #}
    GROUP BY
        block_hour
),
token_swapped AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        msg_index,
        tx_succeeded,
        _inserted_timestamp,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :sender :: STRING AS trader,
        j :pool_id :: STRING AS pool_id,
        j :tokens_in :: STRING AS tokens_in,
        j :tokens_out :: STRING AS tokens_out
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        msg_type = 'token_swapped'

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
GROUP BY
    block_id,
    block_timestamp,
    tx_id,
    msg_index,
    tx_succeeded,
    _inserted_timestamp
)
SELECT
    block_id,
    block_id_hour,
    block_timestamp,
    tx_id,
    msg_index,
    tx_succeeded,
    trader,
    pool_id,
    RIGHT(tokens_in, LENGTH(tokens_in) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(tokens_in, '[^[:digit:]]', ' ')), ' ', 0))) AS from_currency,
    SPLIT_PART(
        TRIM(
            REGEXP_REPLACE(
                tokens_in,
                '[^[:digit:]]',
                ' '
            )
        ),
        ' ',
        0
    ) :: FLOAT AS from_amount,
    RIGHT(tokens_out, LENGTH(tokens_out) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(tokens_out, '[^[:digit:]]', ' ')), ' ', 0))) AS to_currency,
    SPLIT_PART(
        TRIM(
            REGEXP_REPLACE(
                tokens_out,
                '[^[:digit:]]',
                ' '
            )
        ),
        ' ',
        0
    ) :: FLOAT AS to_amount,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id','msg_index']
    ) }} AS token_swapped_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    token_swapped A
    JOIN bhour b
    ON DATE_TRUNC(
        'hour',
        A.block_timestamp
    ) = b.block_hour
