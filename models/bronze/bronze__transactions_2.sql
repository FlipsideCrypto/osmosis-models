{{ config(
  materialized = 'incremental',
  incremental_strategy = 'merge',
  incremental_predicates = ["dynamic_range_predicate", "block_id"],
  unique_key = ['block_id','tx_id'],
  cluster_by = ['_inserted_timestamp::date'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
  tags = ['core']
) }}
-- depends_on: {{ ref('bronze__streamline_transactions') }}
WITH base AS (

  SELECT
    COALESCE(
      DATA :height,
      VALUE :block_number,
      DATA: tx_responses :height
    ) :: INT AS block_id,
    COALESCE(
      DATA :tx_result :codespace,
      DATA :tx_responses :codespace
    ) :: STRING AS codespace,
    COALESCE(
      DATA :tx_result :gas_used,
      DATA :tx_responses :gas_used
    ) :: INT AS gas_used,
    COALESCE(
      DATA :tx_result :gas_wanted,
      DATA :tx_responses :gas_wanted
    ) :: INT AS gas_wanted,
    COALESCE(
      DATA :hash,
      DATA :tx_responses :txhash
    ) :: STRING AS tx_id,
    COALESCE(
      DATA :tx_result :code,
      DATA :tx_responses :code
    ) :: STRING AS tx_code,
    CASE
      WHEN tx_code = 0 THEN TRUE
      ELSE FALSE
    END AS tx_succeeded,
    COALESCE(
      DATA :tx_result :events,
      DATA :tx_responses :events
    ) AS msgs,
    DATA :tx_responses :tx :auth_info AS auth_info,
    DATA :tx_responses :tx :body AS tx_body,
    COALESCE(
      VALUE :BLOCK_NUMBER_REQUESTED,
      REPLACE(
        metadata :request [3] :params :events,
        'tx.height='
      ),
      SUBSTR(
        metadata :request :url,
        CHARINDEX(
          'offset=',
          metadata :request :url
        ) + 7,
        99
      )
    ) AS block_id_requested,
    inserted_timestamp AS _inserted_timestamp
  FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_transactions') }}
{% else %}
  {{ ref('bronze__streamline_FR_transactions') }}
{% endif %}

A
)
SELECT
  A.block_id,
  {# b.block_timestamp, #}
  A.codespace,
  A.gas_used,
  A.gas_wanted,
  A.tx_id,
  A.tx_succeeded,
  A.tx_code,
  A.msgs,
  A.auth_info,
  A.tx_body,
  A.block_id_requested,
  A._inserted_timestamp,
FROM
  base A

{% if is_incremental() %}
WHERE
  A._inserted_timestamp >= (
    SELECT
      MAX(
        _inserted_timestamp
      )
    FROM
      {{ this }}
  )
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY block_id, tx_id
ORDER BY
  _inserted_timestamp DESC)) = 1
