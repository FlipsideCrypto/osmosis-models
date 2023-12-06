{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'MARS',
    'PURPOSE': 'DEFI' }} },
    tags = ['noncore']
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    msg_group,
    msg_sub_group,
    msg_index,
    contract_name,
    action,
    to_address,
    denom AS currency,
    amount :: INT AS amount,
    COALESCE(
        red_bank_actions_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','msg_index']
        ) }}
    ) AS ez_red_bank_rewards_distributions_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__red_bank_actions') }}
WHERE
    action = 'distribute_rewards'
