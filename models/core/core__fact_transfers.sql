{{ config(
    materialized = 'view'
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    transfer_type,
    sender,
    amount,
    currency,
    DECIMAL,
    receiver,
    foreign_address,
    foreign_chain
FROM
    {{ ref('silver__transfers') }}
