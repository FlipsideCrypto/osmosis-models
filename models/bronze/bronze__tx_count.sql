{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = 'id',
  cluster_by = ['_inserted_timestamp::date'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
  tags = ['core']
) }}
-- depends_on: {{ ref('bronze__streamline_tx_counts') }}

SELECT
  id,
  block_number AS block_id,
  DATA :: INT AS num_txs,
  _inserted_timestamp
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_tx_counts') }}
{% else %}
  {{ ref('bronze__streamline_FR_tx_counts') }}
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
