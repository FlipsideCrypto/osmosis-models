{{ config(
  materialized = 'incremental',
  unique_key = "base",
  incremental_strategy = 'delete+insert',
  tags = ['daily']
) }}

WITH base AS (

  SELECT
    resp,
    _inserted_timestamp
  FROM
    {{ ref("bronze_api__github_asset_metadata") }}

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

qualify(ROW_NUMBER() over (
ORDER BY
  _inserted_timestamp DESC) = 1)
)
SELECT
  VALUE :base :: STRING AS base,
  VALUE :coingecko_id :: STRING AS coingecko_id,
  VALUE :denom_units AS denom_units,
  VALUE :description :: STRING AS description,
  VALUE :display :: STRING AS display,
  VALUE :keywords AS keywords,
  VALUE :logo_URIs AS logo_URIs,
  VALUE :name :: STRING AS NAME,
  VALUE :symbol :: STRING AS symbol,
  VALUE :traces: AS traces,
  _inserted_timestamp
FROM
  base,
  LATERAL FLATTEN(resp :data :assets) qualify(ROW_NUMBER() over (PARTITION BY base
ORDER BY
  _inserted_timestamp DESC) = 1)
