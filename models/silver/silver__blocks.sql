{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', chain_id, block_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
) }}

SELECT
  block_id,
  block_timestamp,
  chain_id,
  tx_count,
  header :proposer_address :: STRING AS proposer_address,
  header :validators_hash :: STRING AS validator_hash,
  ingested_at AS _ingested_at
FROM
  {{ ref('bronze__blocks') }}

{% if is_incremental() %}
WHERE
  ingested_at :: DATE >= CURRENT_DATE -2
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY chain_id, block_id
ORDER BY
  ingested_at DESC)) = 1
