{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', chain_id, block_id, tx_id, msg_index, attribute_index)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['ingested_at::DATE'],
) }}

SELECT
  block_id,
  block_timestamp,
  blockchain,
  chain_id,
  tx_id,
  msg_index,
  msg_type,
  b.index AS attribute_index,
  TRY_BASE64_DECODE_STRING(
    b.value :key :: STRING
  ) AS attribute_key,
  TRY_BASE64_DECODE_STRING(
    b.value :value :: STRING
  ) AS attribute_value,
  ingested_at
FROM
  {{ ref('silver__msgs') }} A,
  LATERAL FLATTEN(
    input => A.msg,
    path => 'attributes'
  ) b

{% if is_incremental() %}
WHERE
  ingested_at :: DATE >= CURRENT_DATE -2
{% endif %}
