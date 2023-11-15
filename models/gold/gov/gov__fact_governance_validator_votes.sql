{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'GOVERNANCE' }} },
    tags = ['noncore']
) }}

SELECT
    proposal_id,
    b.label AS validator,
    validator_address,
    memo,
    vote,
    voting_power,
    version,
    created_at,
    updated_at
FROM
    {{ ref('silver__validator_memos') }} A
    LEFT JOIN {{ ref('silver__validator_metadata') }}
    b
    ON A.validator_address = b.address
