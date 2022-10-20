{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

SELECT
    DISTINCT tx_id,
    msg_group,
    msg_sub_group,
    attribute_value AS lock_id,
    _inserted_timestamp,
    concat_ws(
        '-',
        A.tx_id,
        COALESCE(
            A.msg_group,
            -1
        ),
        COALESCE(
            A.msg_sub_group,
            -1
        )
    ) AS _unique_key
FROM
    {{ ref('silver__msg_attributes') }} A
WHERE
    attribute_key IN (
        'period_lock_id',
        'lock_id'
    )

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
