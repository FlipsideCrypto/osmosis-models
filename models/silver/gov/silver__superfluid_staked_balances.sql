{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['noncore'],
    enabled = false
) }}

WITH super AS (

    SELECT
        DISTINCT block_id,
        delegator_address AS address,
        lock_id
    FROM
        {{ ref('silver__superfluid_staking') }}
        s

{% if is_incremental() %}
WHERE
    block_timestamp :: DATE >=(
        SELECT
            DATEADD('day', -2, MAX(block_timestamp))
        FROM
            {{ this }})
        {% endif %}
    ),
    lp_balances AS (
        SELECT
            A.block_id,
            A.block_timestamp,
            A.address,
            A.balance,
            A.currency,
            A.decimal,
            A.lock_id,
            _inserted_timestamp
        FROM
            {{ ref('silver__locked_liquidity_balances') }} A
            JOIN super b
            ON A.block_id = b.block_id
            AND A.address = b.address
            AND A.lock_id = b.lock_id

{% if is_incremental() %}
WHERE
    block_timestamp :: DATE >=(
        SELECT
            DATEADD('day', -2, MAX(block_timestamp))
        FROM
            {{ this }})
        {% endif %}
    )
SELECT
    block_id,
    block_timestamp,
    'superfluid staked' AS balance_type,
    address,
    lock_id,
    currency,
    DECIMAL,
    balance,
    concat_ws(
        '-',
        block_id,
        address,
        lock_id,
        currency
    ) AS _unique_key,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['_unique_key']
    ) }} AS superfluid_staked_balances_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    lp_balances
