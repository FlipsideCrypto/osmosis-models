{{ config(
  materialized = 'incremental',
  unique_key = "tx_id",
  incremental_strategy = 'merge',
  cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(tx_id)"
) }}
-- depends_on: {{ ref('bronze__streamline_transactions') }}

SELECT
  top 1000 VALUE :block_number :: INT AS block_id,
  DATA :tx_responses :timestamp :: timestamp_ntz AS block_timestamp,
  DATA :tx_responses :codespace :: STRING AS codespace,
  DATA :tx_responses :gas_used :: INT AS gas_used,
  DATA :tx_responses :gas_wanted :: INT AS gas_wanted,
  DATA :tx_responses :txhash :: STRING AS tx_id,
  CASE
    WHEN DATA :tx_responses :code :: INT = 0 THEN TRUE
    ELSE FALSE
  END AS tx_succeeded,
  DATA :tx_responses :code :: INT tx_code,
  DATA :tx_responses :events AS msgs,
  DATA :tx_responses :tx :auth_info AS auth_info,
  DATA :tx_responses :tx :body AS tx_body,
  _inserted_timestamp
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_transactions') }}
{% else %}
  {{ ref('bronze__streamline_FR_transactions') }}
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
  _partition_by_block_id = 10000000 qualify(ROW_NUMBER() over(PARTITION BY tx_id
ORDER BY
  _inserted_timestamp DESC)) = 1
