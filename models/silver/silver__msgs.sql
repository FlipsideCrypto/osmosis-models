{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', tx_id, msg_index)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['_ingested_at::DATE'],
) }}

SELECT
  block_id,
  block_timestamp,
  blockchain,
  chain_id,
  tx_id,
  tx_status,
  INDEX AS msg_index,
  VALUE :type :: STRING AS msg_type,
  VALUE AS msg,
  _ingested_at
FROM
  {{ ref('silver__transactions') }} A,
  LATERAL FLATTEN(input => A.msgs)

{% if is_incremental() %}
WHERE
  _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
