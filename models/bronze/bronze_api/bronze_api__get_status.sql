{{ config(
  materialized = 'incremental',
  full_refresh = false,
  tags = ['core']
) }}

SELECT
  live.udf_api (
    'POST',
    '{Service}/rpc',
    OBJECT_CONSTRUCT(
      'Content-Type',
      'application/json'
    ),
    OBJECT_CONSTRUCT(
      'id',
      1,
      'jsonrpc',
      '2.0',
      'method',
      'status'
    ),
    'Vault/prod/osmosis/blockjoy/mainnet'
  ) AS DATA,
  SYSDATE() AS _inserted_timestamp
