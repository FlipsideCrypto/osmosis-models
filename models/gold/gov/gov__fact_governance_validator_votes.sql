{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'GOVERNANCE' }} },
    tags = ['noncore'],
    enabled = false
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
    updated_at,
    COALESCE (
        validator_memos_id,
        {{ dbt_utils.generate_surrogate_key(
            ['proposal_id','validator_address']
        ) }}
    ) AS fact_validator_votes_id,
    COALESCE(
        A.inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        A.modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__validator_memos') }} A
    LEFT JOIN {{ ref('silver__validator_metadata') }}
    b
    ON A.validator_address = b.address
