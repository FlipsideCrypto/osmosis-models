{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = 'id',
  cluster_by = ['_inserted_timestamp::date','block_timestamp::date'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
  tags = ['core']
) }}
-- depends_on: {{ ref('bronze__streamline_transactions') }}

SELECT
  id,
  COALESCE(
    VALUE :block_number,
    DATA: tx_responses :height
  ) :: INT AS block_id,
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
  REPLACE(
    metadata :request [3] :params :events,
    'tx.height='
  ) :: INT AS block_id_requested,
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
