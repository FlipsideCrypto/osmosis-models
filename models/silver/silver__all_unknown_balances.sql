{{ config(
    materialized = 'view',
    post_hook = 'call silver.sp_bulk_get_balances()',
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
        AND block_timestamp :: DATE <= '2022-05-26' -- some snapshot date
),
wallets_per_block AS (
    SELECT
        DISTINCT block_id,
        attribute_value AS address
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        RLIKE(
            attribute_value,
            'osmo\\w{39}'
        )
        AND block_id > 2383300
),
possible_balances_needed AS (
    SELECT
        2383300 AS block_id,
        address
    FROM
        all_wallets
    UNION ALL
    SELECT
        *
    FROM
        wallets_per_block
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
    {{ source('osmosis_external','balances_api') }}
ORDER BY
    block_id
LIMIT
    50000
