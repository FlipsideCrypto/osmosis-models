{{ config(
  materialized = 'incremental',
  unique_key = "_unique_key",
  incremental_strategy = 'merge',
  cluster_by = ['block_timestamp::DATE'],
) }}

SELECT
  block_id,
  block_timestamp,
  chain_id,
  tx_count,
  header :proposer_address :: STRING AS proposer_address,
  header :validators_hash :: STRING AS validator_hash,
  _inserted_timestamp AS _inserted_timestamp,
  concat_ws(
    '-',
    chain_id,
    block_id
  ) _unique_key
FROM
  {{ ref('bronze__blocks') }}

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

qualify(ROW_NUMBER() over(PARTITION BY chain_id, block_id
ORDER BY
  _inserted_timestamp DESC)) = 1
