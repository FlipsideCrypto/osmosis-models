{{ config(
  materialized = 'table'
) }}

SELECT
  VALUE: operator_address :: STRING AS address,
  'osmosis' AS blockchain,
  'flipside' AS creator,
  'operator' AS label_type,
  'validator' AS label_subtype,
  VALUE: moniker :: STRING AS label,
  VALUE: identity :: STRING AS project_name,
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
    ) AS _unique_key, 
  VALUE :update_time :: TIMESTAMP AS _inserted_timestamp
FROM
  {{ source(
    'bronze_streamline',
    'validator_metadata_api'
  ) }}

  qualify(ROW_NUMBER() over(PARTITION BY blockchain, creator, address
ORDER BY
  _inserted_timestamp DESC)) = 1
