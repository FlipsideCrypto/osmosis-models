{{ config(
    materialized = 'view'
) }}

SELECT 
    date, 
    balance_type, 
    address, 
    currency, 
    decimal, 
    balance
FROM 
    {{ ref('silver__daily_balances') }}