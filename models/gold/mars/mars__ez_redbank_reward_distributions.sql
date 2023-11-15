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
    amount :: INT AS amount
FROM
    {{ ref('silver__red_bank_actions') }}
WHERE
    action = 'distribute_rewards'
