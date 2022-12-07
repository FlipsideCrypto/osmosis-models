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
  VALUE :account_address :: STRING AS account_address,
  VALUE :delegator_shares :: NUMBER AS delegator_shares,
  VALUE :jailed :: BOOLEAN AS jailed,
  VALUE :max_change_rate :: NUMBER AS max_change_rate,
  VALUE :max_rate :: NUMBER AS max_rate,
  VALUE :min_self_delegation :: NUMBER AS min_self_delegation,
  VALUE :rank :: NUMBER AS RANK,
  VALUE :uptime :missed_blocks :: NUMBER AS missed_blocks,
  VALUE AS raw_metadata, 
  concat_ws(
        '-',
        address, 
        creator, 
        blockchain
    ) AS _unique_key
FROM
  {{ source(
    'osmosis_external',
    'validator_metadata_api'
  ) }}
