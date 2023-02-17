{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'MARS',
    'PURPOSE': 'DEFI' }} }
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
    USER,
    asset_index,
    denom AS currency,
    rewards_accrued :: INT AS rewards_accrued
FROM
    {{ ref('silver__red_bank_actions') }}
WHERE
    action = 'balance_change'
