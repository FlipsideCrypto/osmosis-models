{{ config(
    materialized = 'view',
    tags = ['noncore'],
    enabled = false
) }}

SELECT
    address,
    blockchain,
    creator,
    label_type,
    label_subtype,
    label,
    project_name,
    account_address,
    delegator_shares,
    jailed,
    max_change_rate,
    max_rate,
    min_self_delegation,
    RANK,
    missed_blocks,
    raw_metadata,
    COALESCE(
        validator_metadata_id,
        {{ dbt_utils.generate_surrogate_key(
            ['address','creator','blockchain']
        ) }}
    ) AS fact_validators_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__validator_metadata') }}
