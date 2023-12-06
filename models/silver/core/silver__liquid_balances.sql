{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', block_id, address, currency)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['balances']
) }}

WITH base AS (

    SELECT
        bal.block_id,
        bl.block_timestamp,
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
    block_timestamp :: DATE >= (
        SELECT
            DATEADD(
                'day',
                -1,
                MAX(
                    block_timestamp :: DATE
                )
            )
        FROM
            {{ this }}
    )
    OR b.value :denom :: STRING IN (
        SELECT
            currency
        FROM
            {{ this }}
        GROUP BY
            currency
        HAVING
            MAX(COALESCE(DECIMAL, -1)) <> MIN(COALESCE(DECIMAL, -1))
    )
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY bal.block_id, bal.address, currency
ORDER BY
    _inserted_timestamp DESC)) = 1
),
tbl AS (

{% if is_incremental() %}
SELECT
    address, balance, block_id, block_timestamp, currency, _inserted_timestamp
FROM
    base
UNION
SELECT
    address, balance, block_id, block_timestamp, currency, _inserted_timestamp
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
    address, balance, block_id, block_timestamp, currency, _inserted_timestamp
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
all_bts AS (
    SELECT
        DISTINCT address,
        block_timestamp,
        block_id,
        _inserted_timestamp
    FROM
        tbl
),
tmp AS (
    SELECT
        ab.address,
        COALESCE(
            bal.balance,
            0
        ) AS balance,
        ab.block_id,
        ab.block_timestamp,
        ac.currency,
        ab._inserted_timestamp
    FROM
        all_bts ab
        JOIN all_currency ac
        ON ab.address = ac.address
        LEFT JOIN tbl bal
        ON ab.address = bal.address
        AND ab.block_timestamp = bal.block_timestamp
        AND ac.currency = bal.currency qualify(ROW_NUMBER() over(PARTITION BY ab.address, ab.block_id, ac.currency
    ORDER BY
        ab._inserted_timestamp DESC)) = 1
),
dense_tmp AS (
    SELECT
        *,
        DENSE_RANK() over(
            PARTITION BY address,
            currency
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
        t.block_id AS t_block_id,
        t.block_timestamp AS t_block_timestamp,
        t.currency AS t_currency,
        t._inserted_timestamp AS t_inserted_timestamp,
        t2.address AS t2_address,
        t2.balance AS t2_balance,
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
        ) over(PARTITION BY COALESCE(t.address, t2.address), COALESCE(t.currency, t2.currency)) AS max_t2_rn
    FROM
        dense_tmp t full
        OUTER JOIN dense_tmp t2
        ON t.address = t2.address
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
    'liquid' AS balance_type,
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
        ELSE b.decimal
    END AS DECIMAL,
    {{ dbt_utils.generate_surrogate_key(
        ['block_id', 'address', 'currency']
    ) }} AS liquid_balances_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
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
