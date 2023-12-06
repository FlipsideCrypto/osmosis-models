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
    sender,
    recipient,
    on_behalf_of,
    denom AS currency,
    amount :: INT amount,
    amount_scaled :: INT amount_scaled,
    COALESCE(
        red_bank_actions_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','msg_index']
        ) }}
    ) AS ez_red_bank_actions_id,
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
    action IN (
        'deposit',
        'withdraw',
        'borrow',
        'repay'
    )
