{{ config(
    materialized = 'view'
) }}

SELECT 
    block_id, 
    block_timestamp, 
    blockchain, 
    chain_id, 
    tx_id, 
    tx_status, 
    transfer_type, 
    sender, 
    amount, 
    currency, 
    decimal, 
    receiver
FROM {{ ref('silver__transfers') }}

WHERE block_timestamp :: date > '2021-06-17'
AND block_timestamp :: date < '2021-12-31'
AND sender = 'osmo1wmsuutkzg0lu7s5vj94j0ws90msy6ju2vyq66m'