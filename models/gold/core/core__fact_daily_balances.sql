{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'BALANCES' }} },
    tags = ['noncore'],
    enabled = false
) }}

SELECT
    DATE,
    balance_type,
    address,
    currency,
    DECIMAL,
    balance,
    COALESCE(
        daily_balances_final_id,
        {{ dbt_utils.generate_surrogate_key(
            ['date', 'address', 'balance_type', 'currency']
        ) }}
    ) AS fact_daily_balances_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__daily_balances_final') }}
