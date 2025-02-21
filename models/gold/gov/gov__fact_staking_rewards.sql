{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STAKING' }} },
    tags = ['noncore'],
    enabled = false
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    tx_caller_address,
    action,
    delegator_address,
    validator_address,
    amount,
    currency,
    DECIMAL,
    COALESCE(
        staking_rewards_id,
        {{ dbt_utils.generate_surrogate_key(
            ['_unique_key']
        ) }}
    ) AS fact_staking_rewards_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__staking_rewards') }}
UNION ALL
SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    tx_caller_address,
    action,
    delegator_address,
    validator_address,
    amount,
    currency,
    DECIMAL,
    staking_rewards_2_id AS fact_staking_rewards_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__staking_rewards_2') }}
