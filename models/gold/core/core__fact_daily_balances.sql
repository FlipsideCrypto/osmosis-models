{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', date, address, balance_type, currency)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['date'],
    tags = ['balances'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'BALANCES' }} }
) }}

SELECT
    DATE,
    balance_type,
    address,
    currency,
    DECIMAL,
    balance
FROM
    {{ ref('silver__daily_balances') }}
WHERE
    balance > 0

{% if is_incremental() %}
AND DATE >= (
    SELECT
        MAX(
            DATE
        )
    FROM
        {{ this }}
) - INTERVAL '48 HOURS'
{% endif %}
