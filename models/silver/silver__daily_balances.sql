{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', date, address, balance_type, currency)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['date'],
) }}

WITH

{% if is_incremental() %}
recent AS (

    SELECT
        DATE,
        balance_type,
        address,
        balance,
        currency,
        DECIMAL
    FROM
        {{ this }}
    WHERE
        DATE = (
            SELECT
                DATEADD('day', -1, MAX(DATE))
            FROM
                {{ this }})
        ),
        NEW AS (
            SELECT
                block_timestamp :: DATE AS DATE,
                balance_type,
                address,
                balance,
                currency,
                DECIMAL,
                1 AS RANK
            FROM
                {{ ref('silver__liquid_balances') }}
            WHERE
                block_timestamp :: DATE >= (
                    SELECT
                        DATEADD('day', -1, MAX(DATE))
                    FROM
                        {{ this }}) qualify(ROW_NUMBER() over (PARTITION BY block_timestamp :: DATE, address, balance_type, currency
                    ORDER BY
                        block_timestamp DESC)) = 1
                    UNION ALL
                    SELECT
                        block_timestamp :: DATE AS DATE,
                        balance_type,
                        address,
                        balance,
                        currency,
                        DECIMAL,
                        1 AS RANK
                    FROM
                        {{ ref('silver__staked_balances') }}
                    WHERE
                        block_timestamp :: DATE >= (
                            SELECT
                                DATEADD('day', -1, MAX(DATE))
                            FROM
                                {{ this }}) qualify(ROW_NUMBER() over (PARTITION BY block_timestamp :: DATE, address, balance_type, currency
                            ORDER BY
                                block_timestamp DESC)) = 1
                            UNION ALL
                            SELECT
                                block_timestamp,
                                balance_type,
                                address,
                                SUM(balance) AS balance,
                                currency,
                                DECIMAL,
                                1 AS RANK
                            FROM
                                (
                                    SELECT
                                        block_timestamp,
                                        balance_type,
                                        address,
                                        balance,
                                        currency,
                                        DECIMAL,
                                        1 AS RANK
                                    FROM
                                        {{ ref('silver__locked_liquidity_balances') }}
                                    WHERE
                                        block_timestamp :: DATE >= (
                                            SELECT
                                                DATEADD('day', -1, MAX(DATE))
                                            FROM
                                                {{ this }})
                                                AND lock_id || '---' || block_timestamp :: DATE :: STRING NOT IN (
                                                    SELECT
                                                        lock_id || '---' || block_timestamp :: DATE :: STRING
                                                    FROM
                                                        {{ ref('silver__superfluid_staked_balances') }}
                                                    WHERE
                                                        block_timestamp :: DATE >= (
                                                            SELECT
                                                                DATEADD('day', -1, MAX(DATE))
                                                            FROM
                                                                {{ this }})
                                                        ) qualify(ROW_NUMBER() over (PARTITION BY block_timestamp :: DATE, address, balance_type, currency, lock_id
                                                    ORDER BY
                                                        block_timestamp DESC)) = 1
                                                    UNION ALL
                                                    SELECT
                                                        block_timestamp,
                                                        balance_type,
                                                        address,
                                                        balance,
                                                        currency,
                                                        DECIMAL,
                                                        1 AS RANK
                                                    FROM
                                                        {{ ref('silver__superfluid_staked_balances') }}
                                                    WHERE
                                                        block_timestamp :: DATE >= (
                                                            SELECT
                                                                DATEADD('day', -1, MAX(DATE))
                                                            FROM
                                                                {{ this }}) qualify(ROW_NUMBER() over (PARTITION BY block_timestamp :: DATE, address, balance_type, currency, lock_id
                                                            ORDER BY
                                                                block_timestamp DESC)) = 1
                                                        ) liq
                                                    GROUP BY
                                                        block_timestamp,
                                                        balance_type,
                                                        address,
                                                        currency,
                                                        DECIMAL
                                                ),
                                                incremental AS (
                                                    SELECT
                                                        DATE,
                                                        balance_type,
                                                        address,
                                                        balance,
                                                        currency,
                                                        DECIMAL
                                                    FROM
                                                        (
                                                            SELECT
                                                                DATE,
                                                                balance_type,
                                                                address,
                                                                balance,
                                                                currency,
                                                                DECIMAL,
                                                                2 AS RANK
                                                            FROM
                                                                recent
                                                            UNION
                                                            SELECT
                                                                DATE,
                                                                balance_type,
                                                                address,
                                                                balance,
                                                                currency,
                                                                DECIMAL,
                                                                1 AS RANK
                                                            FROM
                                                                NEW
                                                        ) qualify(ROW_NUMBER() over (PARTITION BY DATE, address, balance_type, currency
                                                    ORDER BY
                                                        RANK ASC)) = 1
                                                ),
                                            {% endif %}

                                            base AS (

{% if is_incremental() %}
SELECT
    DATE AS block_timestamp, balance_type, address, balance, currency, DECIMAL
FROM
    incremental
{% else %}
SELECT
    block_timestamp, balance_type, address, balance, currency, DECIMAL
FROM
    {{ ref('silver__liquid_balances') }}
UNION ALL
SELECT
    block_timestamp, balance_type, address, balance, currency, DECIMAL
FROM
    {{ ref('silver__staked_balances') }}
UNION ALL
SELECT
    block_timestamp, balance_type, address, SUM(balance) AS balance, currency, DECIMAL
FROM
    (
SELECT
    block_timestamp, balance_type, address, balance, currency, DECIMAL
FROM
    {{ ref('silver__locked_liquidity_balances') }}
WHERE
    lock_id || '---' || block_timestamp :: DATE :: STRING NOT IN (
SELECT
    lock_id || '---' || block_timestamp :: DATE :: STRING
FROM
    {{ ref('silver__superfluid_staked_balances') }})
UNION ALL
SELECT
    block_timestamp, balance_type, address, balance, currency, DECIMAL
FROM
    {{ ref('silver__superfluid_staked_balances') }}) liq
GROUP BY
    block_timestamp, balance_type, address, currency, DECIMAL
{% endif %}),
address_ranges AS (
    SELECT
        address,
        balance_type,
        currency,
        DECIMAL,
        MIN(
            block_timestamp :: DATE
        ) AS min_block_date,
        MAX (
            CURRENT_TIMESTAMP :: DATE
        ) AS max_block_date
    FROM
        base
    GROUP BY
        address,
        balance_type,
        currency,
        DECIMAL
),
ddate AS (
    SELECT
        HOUR :: DATE AS DATE
    FROM
        {{ source(
            'shared2',
            'hours'
        ) }}
    GROUP BY
        DATE
),
all_dates AS (
    SELECT
        d.date,
        A.balance_type,
        A.address,
        A.currency,
        A.decimal
    FROM
        ddate d
        LEFT JOIN address_ranges A
        ON d.date BETWEEN A.min_block_date
        AND A.max_block_date
    WHERE
        A.address IS NOT NULL
),
osmosis_balances AS (
    SELECT
        block_timestamp,
        balance_type,
        address,
        balance,
        currency,
        DECIMAL
    FROM
        base qualify(ROW_NUMBER() over (PARTITION BY block_timestamp :: DATE, address, balance_type, currency
    ORDER BY
        block_timestamp DESC)) = 1
),
balance_temp AS (
    SELECT
        d.date,
        d.balance_type,
        d.address,
        b.balance,
        d.currency,
        d.decimal
    FROM
        all_dates d
        LEFT JOIN osmosis_balances b
        ON d.date = b.block_timestamp :: DATE
        AND d.address = b.address
        AND d.currency = b.currency
        AND d.balance_type = b.balance_type
)
SELECT
    DATE,
    balance_type,
    address,
    currency,
    DECIMAL,
    LAST_VALUE(
        balance ignore nulls
    ) over(
        PARTITION BY address,
        currency,
        balance_type
        ORDER BY
            DATE ASC rows unbounded preceding
    ) AS balance
FROM
    balance_temp