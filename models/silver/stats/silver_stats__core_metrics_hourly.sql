{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_timestamp_hour",
    cluster_by = ['block_timestamp_hour::DATE'],
    tags = ['noncore']
) }}
/* run incremental timestamp value first then use it as a static value */
{% if execute %}

{% if is_incremental() %}
{% set query %}

SELECT
    MIN(DATE_TRUNC('hour', block_timestamp)) block_timestamp_hour
FROM
    {{ ref('silver__transactions_final') }}
WHERE
    block_timestamp IS NOT NULL
    AND modified_timestamp >= DATEADD(
        HOUR,
        -6,(
            SELECT
                MAX(modified_timestamp)
            FROM
                {{ this }}
        )
    ) {% endset %}
    {% set min_block_timestamp_hour = run_query(query).columns [0].values() [0] %}
    {% if not min_block_timestamp_hour or min_block_timestamp_hour == 'None' %}
        {% set min_block_timestamp_hour = '2099-01-01' %}
    {% endif %}
{% endif %}
{% endif %}
SELECT
    DATE_TRUNC(
        'hour',
        block_timestamp
    ) AS block_timestamp_hour,
    COUNT(
        DISTINCT tx_id
    ) AS transaction_count,
    COUNT(
        DISTINCT CASE
            WHEN tx_succeeded THEN tx_id
        END
    ) AS transaction_count_success,
    COUNT(
        DISTINCT CASE
            WHEN NOT tx_succeeded THEN tx_id
        END
    ) AS transaction_count_failed,
    COUNT(
        DISTINCT tx_from
    ) AS unique_from_count,
    SUM(
        CASE
            WHEN fee ILIKE '%uosmo' THEN TRY_CAST(
                SPLIT_PART(
                    TRIM(
                        REGEXP_REPLACE(
                            fee,
                            '[^[:digit:]]',
                            ' '
                        )
                    ),
                    ' ',
                    0
                ) AS INT
            )
        END / pow(
            10,
            6
        )
    ) AS total_fees,
    MAX(inserted_timestamp) AS _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_timestamp_hour']
    ) }} AS core_metrics_hourly_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__transactions_final') }}
WHERE
    block_timestamp_hour < DATE_TRUNC('hour', systimestamp())

{% if is_incremental() %}
AND DATE_TRUNC(
    'hour',
    block_timestamp
) >= '{{ min_block_timestamp_hour }}'
{% endif %}
GROUP BY
    1
