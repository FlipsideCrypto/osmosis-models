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
      'bronze_streamline',
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
  raw_metadata [0] :aliases [0] :: STRING AS alias,
  raw_metadata [array_size(raw_metadata)-1] :exponent :: NUMBER AS DECIMAL,
  raw_metadata,
  COALESCE(
    raw_metadata [0] :aliases [0] :: STRING,
    raw_metadata [0] :denom :: STRING
  ) AS denom,
  concat_ws(
    '-',
    address,
    creator,
    blockchain
  ) AS _unique_key
FROM
  base qualify(ROW_NUMBER() over(PARTITION BY blockchain, creator, address
ORDER BY
  project_name DESC)) = 1
