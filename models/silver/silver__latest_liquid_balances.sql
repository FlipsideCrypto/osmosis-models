{{ config(
  materialized = 'incremental',
  unique_key = "address",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::date']
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
  b._inserted_timestamp
FROM
  {{ ref('silver__liquid_balances') }}
  b
  JOIN max_bn m
  ON b.address = m.address
  AND b.block_timestamp = m.block_timestamp
