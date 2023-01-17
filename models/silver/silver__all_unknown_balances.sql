{{ config(
    materialized = 'view',
) }}

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
        RLIKE(
            attribute_value,
            'osmo\\w{39}'
        )
        OR (
            msg_type = 'message'
            AND attribute_key = 'action'
            AND attribute_value = 'superfluid_delegate'
        )
),
all_wallets AS (
    SELECT
        DISTINCT attribute_value AS address
    FROM
        base
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
        base
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
        DISTINCT liquidity_provider_address AS address
    FROM
        {{ ref('silver__liquidity_provider_actions') }}
    WHERE
        action = 'pool_joined'
    UNION
    SELECT
        DISTINCT b.address
    FROM
        (
            SELECT
                block_id,
                tx_id
            FROM
                base
            WHERE
                msg_type = 'message'
                AND attribute_key = 'action'
                AND attribute_value = 'superfluid_delegate'
        ) A
        INNER JOIN (
            SELECT
                SPLIT_PART(
                    attribute_value,
                    '/',
                    0
                ) address,
                tx_ID
            FROM
                base
            WHERE
                attribute_key = 'acc_seq'
        ) b
        ON A.tx_ID = b.tx_ID
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
    address
FROM
    possible_balances_needed
EXCEPT
SELECT
    DISTINCT block_id,
    address
FROM
    {{ source(
        'bronze_streamline',
        'balances_api'
    ) }}
ORDER BY
    block_id
