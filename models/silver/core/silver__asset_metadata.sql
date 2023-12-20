{{ config(
  materialized = 'incremental',
  unique_key = "address",
  incremental_strategy = 'delete+insert',
  tags = ['daily']
) }}

WITH base AS (

  SELECT
    base AS address,
    NAME AS label,
    symbol AS project_name,
    denom_units AS raw_metadata,
    '2000-01-01' :: datetime AS _inserted_timestamp
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
),
combo AS (
  SELECT
    address,
    label,
    project_name,
    raw_metadata [0] :aliases [0] :: STRING AS alias,
    raw_metadata [array_size(raw_metadata)-1] :exponent :: NUMBER AS DECIMAL,
    raw_metadata,
    COALESCE(
      raw_metadata [0] :aliases [0] :: STRING,
      raw_metadata [0] :denom :: STRING
    ) AS denom,
    address AS _unique_key,
    _inserted_timestamp
  FROM
    base
  UNION ALL
  SELECT
    COALESCE(
      VALUE :denom :: STRING,
      base
    ) AS address,
    NAME AS label,
    symbol AS project_name,
    VALUE :aliases :: STRING AS alias,
    VALUE :exponent :: INT AS DECIMAL,
    denom_units AS raw_metadata,
    COALESCE(
      VALUE [0] :aliases [0] :: STRING,
      VALUE [0] :denom :: STRING
    ) AS denom,
    address AS _unique_key,
    _inserted_timestamp
  FROM
    {{ ref('silver__github_asset_metadata') }},
    LATERAL FLATTEN(
      denom_units,
      outer => TRUE
    )
)
SELECT
  'osmosis' AS blockchain,
  address,
  'flipside' AS creator,
  'token' AS label_type,
  'token_contract' AS label_subtype,
  label,
  project_name,
  alias,
  DECIMAL,
  raw_metadata,
  denom,
  _unique_key,
  _inserted_timestamp,
  {{ dbt_utils.generate_surrogate_key(
    ['_unique_key']
  ) }} AS asset_metadata_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  combo qualify(ROW_NUMBER() over(PARTITION BY address
ORDER BY
  _inserted_timestamp DESC)) = 1
