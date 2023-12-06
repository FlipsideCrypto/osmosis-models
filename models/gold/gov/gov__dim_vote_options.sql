{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'GOVERNANCE' }} },
    tags = ['noncore']
) }}

SELECT
    DISTINCT vote_option AS vote_id,
    CASE
        WHEN vote_option = 1 THEN 'YES'
        WHEN vote_option = 2 THEN 'ABSTAIN'
        WHEN vote_option = 3 THEN 'NO'
        WHEN vote_option = 4 THEN 'NO WITH VETO'
        ELSE 'NULL'
    END AS description,
    {{ dbt_utils.generate_surrogate_key(
        ['vote_option']
    ) }} AS dim_vote_option_id,
    '2000-01-01' AS inserted_timestamp,
    '2000-01-01' AS modified_timestamp
FROM
    {{ ref('silver__governance_votes') }}
