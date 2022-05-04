{{ config(
  materialized = 'view',
  post_hook = "call silver.sp_bulk_get_asset_metadata()"
) }}

SELECT
  DISTINCT A.value :asset_address :: STRING AS address
FROM
  {{ ref('silver__pool_metadata') }},
  TABLE(FLATTEN(assets)) A
WHERE
  --ignore low liquidity pools with unlabeled assets
  pool_id NOT IN (
    290,
    291,
    677,
    684,
    354,
    691,
    694,
    293,
    654,
    647,
    646
  )
EXCEPT
SELECT
  base AS address
FROM
  {{ source(
    'osmosis_external',
    'asset_metadata_api'
  ) }}
