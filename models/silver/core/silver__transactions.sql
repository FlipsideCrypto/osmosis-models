{{ config(
  materialized = 'incremental',
  unique_key = "tx_id",
  incremental_strategy = 'merge',
  incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
  merge_exclude_columns = ["inserted_timestamp"],
  cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE','modified_timestamp::DATE'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(tx_id)",
  tags = ['core']
) }}
-- depends_on: {{ ref('bronze__streamline_transactions') }}+
{% if execute %}

{% if is_incremental() %}
{% set max_its_query %}

SELECT
  MAX(_inserted_timestamp)
FROM
  {{ this }}

  {% endset %}
  {% set max_its = run_query(max_its_query) [0] [0] %}
  {% set min_bd_query %}
SELECT
  MIN(
    block_id
  )
FROM
  (
    SELECT
      block_id
    FROM
      {{ ref('bronze__transactions_2') }}
    WHERE
      _inserted_timestamp >= '{{max_its}}'
    UNION ALL
    SELECT
      block_id
    FROM
      {{ ref('silver__blocks') }}
    WHERE
      _inserted_timestamp >= '{{max_its}}'
  ) {% endset %}
  {% set min_bd = run_query(min_bd_query) [0] [0] %}
{% endif %}
{% endif %}

WITH sl AS (
  SELECT
    A.block_id,
    b.block_timestamp,
    codespace,
    gas_used,
    gas_wanted,
    tx_id,
    tx_succeeded,
    tx_code,
    msgs,
    auth_info,
    tx_body,
    A._inserted_timestamp
  FROM
    {{ ref('bronze__transactions_2') }} A
    LEFT JOIN {{ ref('silver__blocks') }}
    b
    ON A.block_id = b.block_id

{% if is_incremental() %}
WHERE
  A.block_id >= '{{min_bd}}'
  AND COALESCE(
    A.block_id,
    99999999999
  ) >= '{{min_bd}}'
  AND GREATEST(
    A._inserted_timestamp,
    b._inserted_timestamp
  ) >= (
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
  source,
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
  _inserted_timestamp,
  {{ dbt_utils.generate_surrogate_key(
    ['tx_id']
  ) }} AS transactions_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  combo qualify(ROW_NUMBER() over(PARTITION BY tx_id
ORDER BY
  block_id DESC, _inserted_timestamp DESC)) = 1
