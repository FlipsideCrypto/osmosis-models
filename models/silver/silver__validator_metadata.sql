{{ config(
  materialized = 'table'
) }}

SELECT
  operator_address AS address,
  'osmosis' AS blockchain,
  'flipside' AS creator,
  'operator' AS label_type,
  'validator' AS label_subtype,
  moniker AS label,
  identity AS project_name,
  VALUE AS raw_metadata
FROM
  {{ source(
    'osmosis_external',
    'validator_metadata_api'
  ) }}
