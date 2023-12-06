{{ config(
  materialized = 'incremental',
  unique_key = "address",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::date'],
  tags = ['balances']
) }}

WITH max_bn AS (

  SELECT
    address,
    MAX(block_timestamp) AS block_timestamp
  FROM
    {{ ref('silver__liquid_balances') }}
  GROUP BY
    address
)
SELECT
  b.address,
  b.balance,
  b.balance_type,
  b.block_id,
  b.block_timestamp,
  b.decimal,
  b.currency,
  b._inserted_timestamp,
  {{ dbt_utils.generate_surrogate_key(
    ['b.address', 'b.balance_type']
  ) }} AS latest_liquid_balances_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  {{ ref('silver__liquid_balances') }}
  b
  JOIN max_bn m
  ON b.address = m.address
  AND b.block_timestamp = m.block_timestamp
