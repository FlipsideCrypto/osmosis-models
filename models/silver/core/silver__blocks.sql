{{ config(
  materialized = 'incremental',
  unique_key = ['chain_id','block_id'],
  incremental_strategy = 'merge',
  cluster_by = ['block_timestamp::DATE']
) }}
-- depends_on: {{ ref('bronze__streamline_blocks') }}

SELECT
  block_number AS block_id,
  DATA :result :block :header :time :: datetime AS block_timestamp,
  DATA :result :block :header :chain_id :: STRING AS chain_id,
  COALESCE(ARRAY_SIZE(DATA :result :block :data :txs) :: NUMBER, 0) AS tx_count,
  DATA :result :block :header: proposer_address :: STRING AS proposer_address,
  DATA :result :block :header: validators_hash :: STRING AS validator_hash,
  _inserted_timestamp
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_blocks') }}
{% else %}
  {{ ref('bronze__streamline_FR_blocks') }}
{% endif %}

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
WHERE
  _partition_by_block_id = 10000000 qualify(ROW_NUMBER() over(PARTITION BY chain_id, block_id
ORDER BY
  _inserted_timestamp DESC)) = 1
