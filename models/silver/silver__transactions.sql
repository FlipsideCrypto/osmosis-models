{{ config(
  materialized = 'incremental',
  unique_key = "tx_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['_ingested_at::DATE'],
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
  ingested_at AS _ingested_at
FROM
  {{ ref('bronze__transactions') }}

{% if is_incremental() %}
WHERE
  ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY tx_id
ORDER BY
  ingested_at DESC)) = 1
