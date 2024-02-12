{{ config(
    materialized = 'incremental',
    unique_key = ['tx_id','_body_index'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['noncore']
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    trader,
    from_amount,
    from_currency,
    as_f.decimal AS from_decimal,
    to_amount,
    to_currency,
    as_t.decimal AS TO_DECIMAL,
    pool_id :: ARRAY AS pool_ids,
    pool_id :: INT AS pool_id,
    msg_index AS _body_index,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id','msg_index']
    ) }} AS swaps_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    A._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__token_swapped') }} A
    LEFT OUTER JOIN {{ ref('silver__asset_metadata') }}
    as_f
    ON A.from_currency = as_f.address
    LEFT OUTER JOIN {{ ref('silver__asset_metadata') }}
    as_t
    ON A.to_currency = as_t.address

{% if is_incremental() %}
WHERE
    A._inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
