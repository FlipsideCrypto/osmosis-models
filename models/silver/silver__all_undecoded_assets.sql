{{ config(
  materialized = 'view',
  post_hook = "call silver.sp_bulk_get_asset_metadata()"
) }}

SELECT
  DISTINCT A.value :asset_address :: STRING AS address
FROM
  {{ ref('silver__pool_metadata') }},
  TABLE(FLATTEN(assets)) A
EXCEPT
SELECT
  base AS address
FROM
  {{ source(
    'osmosis_external',
    'asset_metadata_api'
  ) }}
