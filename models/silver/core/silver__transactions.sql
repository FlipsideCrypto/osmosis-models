{{ config(
  materialized = 'incremental',
  unique_key = "tx_id",
  incremental_strategy = 'merge',
  cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(tx_id)",
  tags = ['core']
) }}
-- depends_on: {{ ref('bronze__streamline_transactions') }}
WITH sl AS (

  SELECT
    block_id,
    block_timestamp,
    codespace,
    gas_used,
    gas_wanted,
    tx_id,
    tx_succeeded,
    tx_code,
    msgs,
    auth_info,
    tx_body,
    _inserted_timestamp
  FROM
    {{ ref('bronze__transactions_2') }}

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
)

{% if is_incremental() %}
{% else %},
  cw AS (
    SELECT
      block_id,
      block_timestamp,
      tx :tx_result :codespace :: STRING AS codespace,
      tx :tx_result :gasUsed :: INT AS gas_used,
      tx :tx_result :gasWanted :: INT AS gas_wanted,
      tx_id,
      CASE
        WHEN tx :tx_result :code :: INT = 0 THEN TRUE
        ELSE FALSE
      END AS tx_succeeded,
      tx :tx_result :code :: INT tx_code,
      tx :tx_result :events AS msgs,
      tx :auth_info AS auth_info,
      tx :body AS tx_body,
      _inserted_timestamp
    FROM
      {{ source(
        'bronze',
        'chainwalkers_txs'
      ) }}
  ),
  man AS (
    SELECT
      block_id,
      block_timestamp,
      codespace,
      gas_used,
      gas_wanted,
      tx_id,
      tx_succeeded,
      tx_code,
      msgs,
      auth_info,
      tx_body,
      _inserted_timestamp
    FROM
      {{ ref('silver___manual_tx_lq') }}
  )
{% endif %},
combo AS (
  SELECT
    'sl' AS source,
    block_id,
    block_timestamp,
    codespace,
    gas_used,
    gas_wanted,
    tx_id,
    tx_succeeded,
    tx_code,
    msgs,
    auth_info,
    tx_body,
    _inserted_timestamp
  FROM
    sl

{% if is_incremental() %}
{% else %}
  UNION ALL
  SELECT
    'cw' AS source,
    block_id,
    block_timestamp,
    codespace,
    gas_used,
    gas_wanted,
    tx_id,
    tx_succeeded,
    tx_code,
    msgs,
    auth_info,
    tx_body,
    _inserted_timestamp
  FROM
    cw
  UNION ALL
  SELECT
    'man' AS source,
    block_id,
    block_timestamp,
    codespace,
    gas_used,
    gas_wanted,
    tx_id,
    tx_succeeded,
    tx_code,
    msgs,
    auth_info,
    tx_body,
    _inserted_timestamp
  FROM
    man
  {% endif %}
)
SELECT
  *
FROM
  combo qualify(ROW_NUMBER() over(PARTITION BY tx_id
ORDER BY
  block_id DESC, _inserted_timestamp DESC)) = 1
