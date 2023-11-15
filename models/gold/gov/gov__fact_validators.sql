{{ config(
    materialized = 'view',
    tags = ['noncore']
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
    raw_metadata
FROM
    {{ ref('silver__validator_metadata') }}
