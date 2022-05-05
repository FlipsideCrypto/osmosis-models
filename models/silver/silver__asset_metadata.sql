{{ config(
  materialized = 'table'
) }}

WITH base AS (

  SELECT
    base AS address,
    NAME AS label,
    symbol AS project_name,
    denom_units AS raw_metadata
  FROM
    {{ source(
      'osmosis_external',
      'asset_metadata_api'
    ) }}
  GROUP BY
    1,
    2,
    3,
    4
)
SELECT
  'osmosis' AS blockchain,
  address,
  'flipside' AS creator,
  'token' AS label_type,
  'token_contract' AS label_subtype,
  label,
  project_name,
  raw_metadata
FROM
  base
