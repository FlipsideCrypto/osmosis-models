{{ config (
    materialized = "incremental",
    unique_key = ["block_id","address"],
    tags = ['streamline_view']
) }}
-- depends_on: {{ ref('silver__blocks') }}
WITH base AS (

    SELECT
        block_id,
        block_timestamp,
        tx_id,
        msg_type,
        attribute_key,
        attribute_value
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        (
            RLIKE(
                attribute_value,
                'osmo\\w{39}'
            )
            OR (
                msg_type = 'message'
                AND attribute_key = 'action'
                AND attribute_value = 'superfluid_delegate'
            )
        )
        AND block_id > 12768267 -- last block pulled via old process

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
    SELECT
        MAX(
            block_timestamp :: DATE
        )
    FROM
        (
            SELECT
                MAX(block_id) block_id
            FROM
                {{ this }}
        ) A
        JOIN {{ ref('silver__blocks') }}
        b
        ON A.block_id = b.block_id
)
{% endif %}
),
wallets_per_block AS (
    SELECT
        DISTINCT block_id,
        block_timestamp :: DATE AS block_timestamp_date,
        attribute_value AS address
    FROM
        base
    WHERE
        RLIKE(
            attribute_value,
            'osmo\\w{39}'
        )
        AND block_id > 2383300
        AND block_timestamp_date < CURRENT_DATE
),
max_block_id_per_date AS (
    SELECT
        block_timestamp_date,
        MAX(block_id) AS max_block_id
    FROM
        wallets_per_block
    GROUP BY
        1
),
max_block_id_per_date_all AS (
    SELECT
        block_timestamp :: DATE AS block_timestamp_date,
        MAX(block_id) AS max_block_id
    FROM
        base
    WHERE
        block_id > 2383300
    GROUP BY
        1
),
unique_address_per_block_date AS (
    SELECT
        DISTINCT max_block_id,
        address
    FROM
        wallets_per_block b
        LEFT OUTER JOIN max_block_id_per_date d
        ON d.block_timestamp_date = b.block_timestamp_date
),
all_lp_wallets AS (
    SELECT
        liquidity_provider_address AS address
    FROM
        {{ ref('silver__liquidity_provider_actions') }}
    WHERE
        action = 'pool_joined'
    GROUP BY
        1
    UNION
    SELECT
        delegator_address AS address
    FROM
        {{ ref('silver__superfluid_actions') }}
    GROUP BY
        1
),
possible_balances_needed AS (
    SELECT
        max_block_id AS block_id,
        address
    FROM
        unique_address_per_block_date
    UNION
    SELECT
        max_block_id,
        address
    FROM
        all_lp_wallets
        CROSS JOIN (
            SELECT
                DISTINCT max_block_id
            FROM
                unique_address_per_block_date
        )
    UNION
    SELECT
        max_block_id,
        address
    FROM
        {{ ref('bronze__balance_addresses_everyday') }}
        CROSS JOIN (
            SELECT
                DISTINCT max_block_id
            FROM
                max_block_id_per_date_all
        )
)
SELECT
    block_id,
    address,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    possible_balances_needed
