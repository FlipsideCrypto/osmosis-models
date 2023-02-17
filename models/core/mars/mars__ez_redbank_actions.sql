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
    sender,
    recipient,
    on_behalf_of,
    denom AS currency,
    amount :: INT amount,
    amount_scaled :: INT amount_scaled
FROM
    {{ ref('silver__red_bank_actions') }}
WHERE
    action IN (
        'deposit',
        'withdraw',
        'borrow',
        'repay'
    )
