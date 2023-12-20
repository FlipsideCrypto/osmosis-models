{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_id', 'address', 'currency'],
    cluster_by = ['block_timestamp'],
    tags = ['noncore']
) }}

WITH all_staked AS (

    SELECT
        block_id,
        block_timestamp,
        delegator_address AS address,
        amount,
        currency,
        CASE
            WHEN currency LIKE 'gamm/pool/%' THEN 18
            ELSE A.decimal
        END AS DECIMAL,
        s._inserted_timestamp
    FROM
        {{ ref('silver__staking') }}
        s
        LEFT OUTER JOIN {{ ref('silver__asset_metadata') }} A
        ON s.currency = A.address
    WHERE
        action = 'delegate'

{% if is_incremental() %}
AND block_timestamp :: DATE >=(
    SELECT
        DATEADD('day', -2, MAX(block_timestamp))
    FROM
        {{ this }})
    {% endif %}
    UNION ALL
    SELECT
        block_id,
        block_timestamp,
        delegator_address AS address,- amount,
        currency,
        CASE
            WHEN currency LIKE 'gamm/pool/%' THEN 18
            ELSE A.decimal
        END AS DECIMAL,
        s._inserted_timestamp
    FROM
        {{ ref('silver__staking') }}
        s
        LEFT OUTER JOIN {{ ref('silver__asset_metadata') }} A
        ON s.currency = A.address
    WHERE
        action = 'undelegate'

{% if is_incremental() %}
AND block_timestamp :: DATE >=(
    SELECT
        DATEADD('day', -2, MAX(block_timestamp))
    FROM
        {{ this }})
    {% endif %}
)
SELECT
    block_id,
    block_timestamp,
    'staked' AS balance_type,
    address,
    currency,
    DECIMAL,
    SUM(amount) over(
        PARTITION BY address,
        currency
        ORDER BY
            block_timestamp ASC rows unbounded preceding
    ) AS balance,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_id', 'address', 'currency']
    ) }} AS staked_balances_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_staked
