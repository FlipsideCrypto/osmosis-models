{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', chain_id, block_id, tx_id, msg_index, attribute_index)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['ingested_at::DATE'],
) }}

WITH base AS (

  SELECT
    block_id,
    block_timestamp,
    blockchain,
    chain_id,
    tx_id,
    msg_index,
    msg_type,
    SUBSTRING(path, POSITION('[', path) + 1, POSITION(']', path) - POSITION('[', path) -1) AS attribute_index,
    key AS xKey,
    TRY_BASE64_DECODE_STRING(
      VALUE :: STRING
    ) AS xValue,
    ingested_at
  FROM
    {{ ref('silver__msgs') }} A,
    LATERAL FLATTEN(
      input => A.msg,
      recursive => TRUE
    )
  WHERE
    key IN ('key', 'value')

{% if is_incremental() %}
AND ingested_at :: DATE >= getdate() - INTERVAL '2 days'
{% endif %}
)
SELECT
  p.block_id,
  p.block_timestamp,
  p.blockchain,
  p.chain_id,
  p.tx_id,
  p.msg_index,
  p.msg_type,
  p.attribute_index,
  attribute_key,
  attribute_value,
  ingested_at
FROM
  base A pivot(MAX(xvalue) for xKey IN ('key', 'value')) AS p (
    block_id,
    block_timestamp,
    blockchain,
    chain_id,
    tx_id,
    msg_index,
    msg_type,
    attribute_index,
    ingested_at,
    attribute_key,
    attribute_value
  )
