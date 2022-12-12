{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', block_id, address, currency)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp'],
) }}

WITH base AS (

    SELECT
        bal.block_id,
        bl.block_timestamp,
        'liquid' AS balance_type,
        bal.address,
        b.value :denom :: STRING AS currency,
        b.value :amount :: INT AS balance,
        TO_TIMESTAMP_NTZ(
            SUBSTR(SPLIT_PART(metadata$filename, '/', 4), 1, 10) :: NUMBER,
            0
        ) AS _inserted_timestamp
    FROM
        {{ source(
            'bronze_streamline',
            'balances_api'
        ) }}
        bal
        LEFT OUTER JOIN {{ ref('silver__blocks') }}
        bl
        ON bal.block_id = bl.block_id
        LEFT OUTER JOIN TABLE(FLATTEN (input => balances, outer => TRUE)) b

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY bal.block_id, bal.address, currency
ORDER BY
    _inserted_timestamp DESC)) = 1
),
tbl AS (

{% if is_incremental() %}
SELECT
    address, balance, balance_type, block_id, block_timestamp, currency, _inserted_timestamp
FROM
    base
UNION
SELECT
    address, balance, balance_type, block_id, block_timestamp, currency, _inserted_timestamp
FROM
    silver.latest_liquid_balances
WHERE
    address IN (
SELECT
    DISTINCT address
FROM
    base)
{% else %}
SELECT
    address, balance, balance_type, block_id, block_timestamp, currency, _inserted_timestamp
FROM
    base
{% endif %}),
all_currency AS (
    SELECT
        DISTINCT address,
        currency
    FROM
        tbl
),
tmp AS (
    SELECT
        A.address,
        COALESCE(
            A.balance,
            0
        ) AS balance,
        A.balance_type,
        A.block_id,
        A.block_timestamp,
        COALESCE(
            A.currency,
            b.currency
        ) AS currency,
        A._inserted_timestamp
    FROM
        tbl A
        LEFT JOIN all_currency b
        ON A.currency IS NULL
        AND A.address = b.address qualify(ROW_NUMBER() over(PARTITION BY A.address, balance_type, block_id, COALESCE(A.currency, b.currency)
    ORDER BY
        _inserted_timestamp DESC)) = 1
),
dense_tmp AS (
    SELECT
        *,
        DENSE_RANK() over(
            PARTITION BY address,
            currency,
            balance_type
            ORDER BY
                block_id ASC
        ) AS rn
    FROM
        tmp
),
joined AS (
    SELECT
        t.address AS t_address,
        t.balance AS t_balance,
        t.balance_type AS t_balance_type,
        t.block_id AS t_block_id,
        t.block_timestamp AS t_block_timestamp,
        t.currency AS t_currency,
        t._inserted_timestamp AS t_inserted_timestamp,
        t2.address AS t2_address,
        t2.balance AS t2_balance,
        t2.balance_type AS t2_balance_type,
        t2.block_id AS t2_block_id,
        t2.block_timestamp AS t2_block_timestamp,
        t2.currency AS t2_currency,
        t2._inserted_timestamp AS t2_inserted_timestamp,
        LAST_VALUE(
            t2.block_timestamp ignore nulls
        ) over (
            PARTITION BY COALESCE(
                t.address,
                t2.address
            ),
            t.rn
            ORDER BY
                t2.block_timestamp
        ) AS block_timestamp,
        LAST_VALUE(
            t2.block_id ignore nulls
        ) over (
            PARTITION BY COALESCE(
                t.address,
                t2.address
            ),
            t.rn
            ORDER BY
                t2.block_timestamp
        ) AS block_id,
        t.rn AS t_rn,
        t2.rn AS t2_rn,
        MAX(
            t2.rn
        ) over(PARTITION BY COALESCE(t.address, t2.address), COALESCE(t.balance_type, t2.balance_type), COALESCE(t.currency, t2.currency)) AS max_t2_rn
    FROM
        dense_tmp t full
        OUTER JOIN dense_tmp t2
        ON t.address = t2.address
        AND t.balance_type = t2.balance_type
        AND t.currency = t2.currency
        AND t.rn = t2.rn - 1
)
SELECT
    COALESCE(
        t2_address,
        t_address
    ) AS address,
    COALESCE(
        t2_balance,
        0
    ) AS balance,
    COALESCE(
        t2_balance_type,
        t_balance_type
    ) AS balance_type,
    COALESCE(
        t2_currency,
        t_currency
    ) AS currency,
    COALESCE(
        t2_block_id,
        block_id
    ) AS block_id,
    COALESCE(
        t2_block_timestamp,
        block_timestamp
    ) AS block_timestamp,
    COALESCE(
        t2_inserted_timestamp,
        t_inserted_timestamp
    ) AS _inserted_timestamp,
    CASE
        WHEN currency LIKE 'gamm/pool/%' THEN 18
        ELSE b.raw_metadata [1] :exponent
    END AS DECIMAL
FROM
    joined A
    LEFT OUTER JOIN {{ ref('silver__asset_metadata') }}
    b
    ON COALESCE(
        t2_currency,
        t_currency
    ) = b.address
WHERE
    COALESCE(
        t_rn,
        0
    ) <> max_t2_rn
    AND COALESCE(
        t2_currency,
        t_currency
    ) IS NOT NULL
