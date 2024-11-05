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
    action,
    delegator_address,
    amount,
    currency,
    DECIMAL,
    validator_address,
    lock_id,
    IFF(
        action = 'undelegate'
        AND currency IS NOT NULL,
        TRUE,
        FALSE
    ) AS is_unpool,
    COALESCE(
        superfluid_staking_id,
        {{ dbt_utils.generate_surrogate_key(
            ['_unique_key']
        ) }}
    ) AS fact_superfluid_staking_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__superfluid_staking') }}
UNION ALL
SELECT
    A.block_id,
    A.block_timestamp,
    A.tx_id,
    A.tx_succeeded,
    A.msg_action AS action,
    A.locker_address AS delegator_address,
    amount,
    currency,
    DECIMAL,
    A.validator_address,
    A.lock_id,
    FALSE AS is_unpool,
    COALESCE(
        locked_liquidity_actions_convert_id,
        {{ dbt_utils.generate_surrogate_key(
            ['_unique_key']
        ) }}
    ) AS fact_superfluid_staking_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__locked_liquidity_actions_convert') }} A
