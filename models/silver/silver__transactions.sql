{{ config(
  materialized = 'incremental',
  unique_key = "tx_id",
  incremental_strategy = 'merge',
  cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
) }}

SELECT
  block_id,
  block_timestamp,
  'osmosis' AS blockchain,
  chain_id,
  tx :tx_result :codespace :: STRING AS codespace,
  tx :tx_result :gasUsed :: INT AS gas_used,
  tx :tx_result :gasWanted :: INT AS gas_wanted,
  tx_id,
  CASE
    WHEN tx :tx_result :code :: INT = 0 THEN 'SUCCEEDED'
    ELSE 'FAILED'
  END AS tx_status,
  tx :tx_result :code :: INT tx_code,
  tx :tx_result :events AS msgs,
  tx :auth_info AS auth_info,
  tx :body AS tx_body,
  ingested_at AS _inserted_timestamp
FROM
  {{ ref('bronze__transactions') }}
WHERE
  block_id = tx :height :: INT

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

qualify(ROW_NUMBER() over(PARTITION BY tx_id
ORDER BY
  ingested_at DESC)) = 1
