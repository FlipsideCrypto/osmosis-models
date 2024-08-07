{{ config(
  materialized = 'table',
  tags = ['daily']
) }}

SELECT
  live.udf_api(
    'GET',
    'https://raw.githubusercontent.com/osmosis-labs/assetlists/main/osmosis-1/generated/chain_registry/assetlist.json',{},{}
  ) AS resp,
  SYSDATE() AS _inserted_timestamp,
  '{{ invocation_id }}' AS _invocation_id
