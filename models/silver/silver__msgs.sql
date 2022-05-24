{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', tx_id, msg_index)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['_ingested_at::DATE'],
) }}

WITH b AS (

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
    IFF(
      TRY_BASE64_DECODE_STRING(
        msg :attributes [0] :key :: STRING
      ) = 'action',
      TRUE,
      FALSE
    ) AS is_action,
    _ingested_at
  FROM
    {{ ref('silver__transactions') }} A,
    LATERAL FLATTEN(input => A.msgs)

{% if is_incremental() %}
WHERE
  _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
)
SELECT
  block_id,
  block_timestamp,
  blockchain,
  chain_id,
  tx_id,
  tx_status,
  NULLIF(
    (conditional_true_event(is_action) over (PARTITION BY tx_id
    ORDER BY
      msg_index) -1),
      -1
  ) AS msg_group,
  msg_index,
  msg_type,
  msg,
  _ingested_at
FROM
  b
