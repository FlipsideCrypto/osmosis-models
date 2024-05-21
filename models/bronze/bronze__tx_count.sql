{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = 'block_id',
  cluster_by = ['_inserted_timestamp::date'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
  tags = ['core']
) }}
-- depends_on: {{ ref('bronze__streamline_tx_counts') }}

SELECT
  COALESCE(
    VALUE :BLOCK_NUMBER :: INT,
    TRY_CAST(
      REPLACE(
        COALESCE(
          metadata :request :data :params [0] :: STRING,
          metadata :request :params [0] :: STRING
        ),
        'tx.height='
      ) AS INT
    )
  ) AS block_id,
  COALESCE(
    DATA :result :total_count,
    DATA
  ) :: INT AS num_txs,
  inserted_timestamp AS _inserted_timestamp
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
