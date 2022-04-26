{{ config(
  materialized = 'incremental',
  unique_key = "block_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['ingested_at::DATE'],
) }}

SELECT 
    block_id, 
    block_timestamp, 
    chain_id, 
    tx_count, 
    header:proposer_address :: STRING AS proposer_address,
    header:validators_hash :: STRING AS validator_hash, 
    ingested_at
FROM {{ ref('bronze__blocks') }}

{% if is_incremental() %}
    WHERE ingested_at :: DATE >= getdate() - INTERVAL '2 days'
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY block_id
ORDER BY
  ingested_at DESC)) = 1