{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STAKING' }} },
    tags = ['noncore']
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    tx_caller_address,
    msg_group,
    amount,
    currency,
    validator_address_operator,
    validator_address_reward
FROM
    {{ ref('silver__validator_commission') }}
