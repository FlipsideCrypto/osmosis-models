{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-',block_id,address,denom)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
) }}

SELECT
    bal.block_id,
    blk.block_timestamp,
    bal.address,
    f.value :denom :: STRING AS denom,
    f.value :amount :: bigint AS amount,
    TO_TIMESTAMP_NTZ(SUBSTR(SPLIT_PART(metadata$filename, '/', 4), 1, 10)) AS _inserted_timestamp
FROM
    {{ source(
        'osmosis_external',
        'balances_api'
    ) }} bal
LEFT OUTER JOIN {{ ref('silver__blocks') }} blk on blk.block_id = bal.block_id
LEFT OUTER JOIN TABLE(FLATTEN(balances)) f

{% if is_incremental() %}
WHERE
    _inserted_date >= (
        SELECT
            MAX(_inserted_timestamp) :: DATE
        FROM
            {{ this }}
    )
AND 
    TO_TIMESTAMP_NTZ(SUBSTR(SPLIT_PART(metadata$filename, '/', 4), 1, 10)) >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
