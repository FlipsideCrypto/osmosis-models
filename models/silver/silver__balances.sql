{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, address, currency)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['date'],
) }}

SELECT 
    bal.block_id, 
    bl.block_timestamp :: date AS date, 
    bal.address, 
    b.value:amount :: INT AS balance, 
    b.value:denom :: STRING AS currency,
    CASE 
        WHEN currency LIKE 'gamm/pool/%' THEN 18
        ELSE raw_metadata[1]:exponent
    END AS decimal, 
    _inserted_date
FROM {{ source(
      'osmosis_external',
      'balances_api'
    ) }} bal 

LEFT OUTER JOIN {{ ref('silver__blocks') }} bl 
ON bal.block_id = bl.block_id 

LEFT OUTER JOIN TABLE(FLATTEN (
    input => balances
)) b

LEFT OUTER JOIN {{ ref('silver__asset_metadata') }} a
ON b.value:denom :: STRING = a.address

{% if is_incremental() %}
WHERE
    _inserted_at :: DATE >= CURRENT_DATE -2
    _ingested_at :: DATE >= CURRENT_DATE -2
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY bal.block_id, bal.address, currency
ORDER BY
  project_name DESC)) = 1