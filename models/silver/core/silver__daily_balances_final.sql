{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', date, address, balance_type, currency)",
    incremental_strategy = 'delete+insert',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(address)",
    cluster_by = ['date'],
    tags = ['balances'],
    enabled = false
) }}

SELECT
    DATE,
    balance_type,
    address,
    currency,
    DECIMAL,
    balance,
    {{ dbt_utils.generate_surrogate_key(
        ['date', 'address', 'balance_type', 'currency']
    ) }} AS daily_balances_final_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
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
