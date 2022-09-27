{{ config(
    materialized = 'view'
) }}

WITH all_wallets AS (

    SELECT
        DISTINCT attribute_value AS address
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        RLIKE(
            attribute_value,
            'osmo\\w{39}'
        )
        AND block_timestamp :: DATE <= '2022-05-31' -- some snapshot date
),
wallets_per_block AS (
    SELECT
        DISTINCT block_id,
        block_timestamp :: DATE AS block_timestamp_date,
        attribute_value AS address
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        RLIKE(
            attribute_value,
            'osmo\\w{39}'
        )
        AND block_id > 2383300
        AND block_timestamp_date < CURRENT_DATE - 1
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
        DISTINCT liquidity_provider_address AS address
    FROM
        {{ ref('silver__liquidity_provider_actions') }}
    WHERE
        action = 'pool_joined'
),
possible_balances_needed AS (
    SELECT
        2383300 AS block_id,
        address
    FROM
        all_wallets
    UNION
    SELECT
        *
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
)
SELECT
    block_id,
    address
FROM
    possible_balances_needed
EXCEPT
SELECT
    DISTINCT block_id,
    address
FROM
    {{ source(
        'osmosis_external',
        'balances_api'
    ) }}
ORDER BY
    block_id
LIMIT
    100000
