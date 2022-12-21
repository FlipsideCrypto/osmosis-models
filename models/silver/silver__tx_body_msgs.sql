{{ config(
  materialized = 'incremental',
  unique_key = "_unique_key",
  incremental_strategy = 'merge',
  cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH b AS (

  SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    INDEX AS msg_group,
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
  tx_id,
  tx_succeeded,
  msg_group,
  msg_type,
  msg,
  concat_ws(
    '-',
    tx_id,
    msg_group
  ) AS _unique_key,
  _inserted_timestamp
FROM
  b
