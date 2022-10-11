{{ config(
  materialized = 'incremental',
  unique_key = "_unique_key",
  incremental_strategy = 'merge',
  cluster_by = ['_inserted_timestamp::DATE'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
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
    VALUE :"@type" :: STRING AS msg_type,
    VALUE AS msg,
    _inserted_timestamp
  FROM
    {{ ref('silver__transactions') }} A,
    LATERAL FLATTEN(
      input => A.tx_body :messages
    )

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(
        _inserted_timestamp
      )
    FROM
      {{ this }}
  )
{% endif %}
)
SELECT
  block_id,
  block_timestamp,
  blockchain,
  chain_id,
  tx_id,
  tx_status,
  msg_index,
  msg_type,
  msg,
  concat_ws(
    '-',
    tx_id,
    msg_index
  ) AS _unique_key,
  _inserted_timestamp
FROM
  b
