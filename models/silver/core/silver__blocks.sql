{{ config(
  materialized = 'incremental',
  unique_key = ['chain_id','block_id'],
  incremental_strategy = 'merge',
  merge_exclude_columns = ["inserted_timestamp"],
  cluster_by = ['block_timestamp::DATE'],
  tags = ['core']
) }}
-- depends_on: {{ ref('bronze__streamline_blocks') }}

SELECT
  DATA :result :block :header :height :: INT AS block_id,
  DATA :result :block :header :time :: datetime AS block_timestamp,
  DATA :result :block :header :chain_id :: STRING AS chain_id,
  COALESCE(ARRAY_SIZE(DATA :result :block :data :txs) :: NUMBER, 0) AS tx_count,
  DATA :result :block :header: proposer_address :: STRING AS proposer_address,
  DATA :result :block :header: validators_hash :: STRING AS validator_hash,
  inserted_timestamp AS _inserted_timestamp,
  {{ dbt_utils.generate_surrogate_key(
    ['block_id']
  ) }} AS blocks_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_blocks') }}
{% else %}
  {{ ref('bronze__streamline_FR_blocks') }}
{% endif %}
WHERE
  chain_id IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp >= (
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
