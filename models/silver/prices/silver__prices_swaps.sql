{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', block_hour, currency)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_hour']
) }}

WITH swaps AS (

    SELECT
        DATE_TRUNC(
            'HOUR',
            block_timestamp
        ) AS block_hour,
        from_currency,
        TRY_TO_NUMBER(from_amount) / pow(
            10,
            f.raw_metadata [1] :exponent
        ) AS from_amount,
        f.project_name AS from_project_name,
        to_currency,
        TRY_TO_NUMBER(to_amount) / pow(
            10,
            t.raw_metadata [1] :exponent
        ) AS to_amount,
        t.project_name AS to_project_name,
        'osmosis' AS dex
    FROM
        {{ ref('silver__swaps') }}
        s
        INNER JOIN {{ ref('silver__asset_metadata') }}
        f
        ON from_currency = f.address
        INNER JOIN {{ ref('silver__asset_metadata') }}
        t
        ON to_currency = t.address
    WHERE
        TRY_TO_NUMBER(from_amount) > 0
        AND TRY_TO_NUMBER(to_amount) > 0
        AND f.raw_metadata [1] :exponent IS NOT NULL
        AND t.raw_metadata [1] :exponent IS NOT NULL

{% if is_incremental() %}
AND block_hour >=(
    SELECT
        DATEADD('day', -1, MAX(block_hour :: DATE))
    FROM
        {{ this }}
)
{% endif %}

qualify(RANK() over(
ORDER BY
    block_hour DESC)) <> 1
),
swap_range AS (
    SELECT
        MIN(
            block_hour
        ) min_date,
        MAX(
            block_hour
        ) max_date
    FROM
        swaps
),
usd AS (
    SELECT
        block_hour,
        from_currency,
        from_project_name,
        from_amount,
        CASE
            WHEN from_currency IN (
                'ibc/9F9B07EF9AD291167CF5700628145DE1DEB777C2CFC7907553B24446515F6D0E',
                'ibc/8242AD24008032E457D2E12D46588FD39FB54FB29680C6C7663D296B383C37C4',
                'ibc/D189335C6E4A68B513C10AB227BF1C1D38C746766278BA3EEB4FB14124F1D858',
                'ibc/0CD3A0285E1341859B5E86B6AB7682F023D03E97607CCC1DC95706411D866DF7'
            ) THEN 1
            ELSE to_amount / NULLIF(
                from_amount,
                0
            )
        END AS from_usd,
        to_currency,
        to_project_name,
        to_amount,
        CASE
            WHEN to_currency IN (
                'ibc/9F9B07EF9AD291167CF5700628145DE1DEB777C2CFC7907553B24446515F6D0E',
                'ibc/8242AD24008032E457D2E12D46588FD39FB54FB29680C6C7663D296B383C37C4',
                'ibc/D189335C6E4A68B513C10AB227BF1C1D38C746766278BA3EEB4FB14124F1D858',
                'ibc/0CD3A0285E1341859B5E86B6AB7682F023D03E97607CCC1DC95706411D866DF7'
            ) THEN 1
            ELSE from_amount / NULLIF(
                to_amount,
                0
            )
        END AS to_usd,
        dex
    FROM
        swaps A
),
usd_2 AS (
    SELECT
        block_hour,
        from_currency AS currency,
        from_project_name AS project_name,
        from_usd AS price
    FROM
        usd
    WHERE
        to_currency IN (
            'ibc/9F9B07EF9AD291167CF5700628145DE1DEB777C2CFC7907553B24446515F6D0E',
            'ibc/8242AD24008032E457D2E12D46588FD39FB54FB29680C6C7663D296B383C37C4',
            'ibc/D189335C6E4A68B513C10AB227BF1C1D38C746766278BA3EEB4FB14124F1D858',
            'ibc/0CD3A0285E1341859B5E86B6AB7682F023D03E97607CCC1DC95706411D866DF7'
        )
    UNION ALL
    SELECT
        block_hour,
        to_currency AS currency,
        to_project_name AS project_name,
        to_usd AS price
    FROM
        usd
    WHERE
        from_currency IN (
            'ibc/9F9B07EF9AD291167CF5700628145DE1DEB777C2CFC7907553B24446515F6D0E',
            'ibc/8242AD24008032E457D2E12D46588FD39FB54FB29680C6C7663D296B383C37C4',
            'ibc/D189335C6E4A68B513C10AB227BF1C1D38C746766278BA3EEB4FB14124F1D858',
            'ibc/0CD3A0285E1341859B5E86B6AB7682F023D03E97607CCC1DC95706411D866DF7'
        )
),
usd_3 AS (
    SELECT
        block_hour,
        currency,
        project_name,
        price,
        STDDEV(TO_NUMERIC(price)) over (
            PARTITION BY currency,
            block_hour
        ) stddev_price -- TO_NUMERIC USED TO FIX ERROR RELATED TO TOO BIG OF INTEGER
    FROM
        usd_2
    WHERE
        currency = 'uosmo'
),
usd_4 AS (
    SELECT
        block_hour,
        currency,
        project_name,
        price,
        stddev_price,
        CASE
            WHEN ABS(price - AVG(price) over(PARTITION BY currency, block_hour)) > stddev_price * 2 THEN TRUE
            ELSE FALSE
        END exclude_from_pricing,
        AVG(price) over(
            PARTITION BY currency,
            block_hour
        ) avg_price
    FROM
        usd_3
),
osmo_price_hour AS (
    SELECT
        block_hour,
        AVG(
            CASE
                WHEN exclude_from_pricing = FALSE THEN price
            END
        ) price
    FROM
        usd_4
    GROUP BY
        block_hour
),
osmo AS (
    SELECT
        A.block_hour,
        from_currency,
        from_project_name,
        from_amount,
        CASE
            WHEN to_currency = 'uosmo' THEN (
                to_amount * prices.price
            ) / NULLIF(
                from_amount,
                0
            )
        END AS from_usd,
        to_currency,
        to_project_name,
        to_amount,
        CASE
            WHEN from_currency = 'uosmo' THEN (
                from_amount * prices.price
            ) / NULLIF(
                to_amount,
                0
            )
        END AS to_usd,
        dex
    FROM
        swaps A
        LEFT JOIN osmo_price_hour prices
        ON A.block_hour = prices.block_hour
    WHERE
        (
            A.from_currency = 'uosmo'
            OR A.to_currency = 'uosmo'
        )
        AND NOT (
            A.from_currency = 'uosmo'
            AND A.to_currency = 'uosmo'
        )
),
combo_1 AS (
    SELECT
        block_hour,
        from_currency AS currency,
        from_project_name AS project_name,
        from_usd AS price,
        dex,
        from_amount AS amount
    FROM
        usd
    WHERE
        to_currency IN (
            'ibc/9F9B07EF9AD291167CF5700628145DE1DEB777C2CFC7907553B24446515F6D0E',
            'ibc/8242AD24008032E457D2E12D46588FD39FB54FB29680C6C7663D296B383C37C4',
            'ibc/D189335C6E4A68B513C10AB227BF1C1D38C746766278BA3EEB4FB14124F1D858',
            'ibc/0CD3A0285E1341859B5E86B6AB7682F023D03E97607CCC1DC95706411D866DF7'
        )
    UNION ALL
    SELECT
        block_hour,
        to_currency AS currency,
        to_project_name AS project_name,
        to_usd AS price,
        dex,
        to_amount AS amount
    FROM
        usd
    WHERE
        from_currency IN (
            'ibc/9F9B07EF9AD291167CF5700628145DE1DEB777C2CFC7907553B24446515F6D0E',
            'ibc/8242AD24008032E457D2E12D46588FD39FB54FB29680C6C7663D296B383C37C4',
            'ibc/D189335C6E4A68B513C10AB227BF1C1D38C746766278BA3EEB4FB14124F1D858',
            'ibc/0CD3A0285E1341859B5E86B6AB7682F023D03E97607CCC1DC95706411D866DF7'
        )
    UNION ALL
    SELECT
        block_hour,
        to_currency AS currency,
        to_project_name AS project_name,
        CASE
            WHEN to_currency IN (
                'ibc/9F9B07EF9AD291167CF5700628145DE1DEB777C2CFC7907553B24446515F6D0E',
                'ibc/8242AD24008032E457D2E12D46588FD39FB54FB29680C6C7663D296B383C37C4',
                'ibc/D189335C6E4A68B513C10AB227BF1C1D38C746766278BA3EEB4FB14124F1D858',
                'ibc/0CD3A0285E1341859B5E86B6AB7682F023D03E97607CCC1DC95706411D866DF7'
            ) THEN 1
            ELSE to_usd
        END price,
        dex,
        to_amount AS amount
    FROM
        osmo
    WHERE
        from_currency = 'uosmo'
    UNION ALL
    SELECT
        block_hour,
        from_currency AS currency,
        from_project_name AS project_name,
        CASE
            WHEN from_currency IN (
                'ibc/9F9B07EF9AD291167CF5700628145DE1DEB777C2CFC7907553B24446515F6D0E',
                'ibc/8242AD24008032E457D2E12D46588FD39FB54FB29680C6C7663D296B383C37C4',
                'ibc/D189335C6E4A68B513C10AB227BF1C1D38C746766278BA3EEB4FB14124F1D858',
                'ibc/0CD3A0285E1341859B5E86B6AB7682F023D03E97607CCC1DC95706411D866DF7'
            ) THEN 1
            ELSE from_usd
        END price,
        dex,
        from_amount AS amount
    FROM
        osmo
    WHERE
        to_currency = 'uosmo'
),
combo_2 AS (
    SELECT
        block_hour,
        currency,
        project_name,
        price,
        MEDIAN(
            price
        ) over (
            PARTITION BY currency,
            block_hour
        ) stddev_price,
        dex,
        amount
    FROM
        combo_1
),
combo_3 AS (
    SELECT
        block_hour,
        currency,
        project_name,
        price,
        stddev_price,
        CASE
            WHEN ABS(
                price - stddev_price
            ) > stddev_price * 2 THEN TRUE
            ELSE FALSE
        END exclude_from_pricing,
        AVG(price) over(
            PARTITION BY currency,
            block_hour
        ) avg_price,
        dex,
        amount
    FROM
        combo_2
),
final_dex AS (
    SELECT
        block_hour,
        block_hour :: DATE AS block_date,
        currency,
        project_name,
        dex,
        AVG(
            CASE
                WHEN exclude_from_pricing = FALSE THEN price
            END
        ) avg_price_usd_hour_excludes,
        MIN(
            price
        ) min_price_usd_hour,
        MAX(
            price
        ) max_price_usd_hour,
        MAX(
            price
        ) - MIN(
            price
        ) AS volatility_measure,
        SUM(
            CASE
                WHEN exclude_from_pricing = FALSE THEN 1
            END
        ) swaps_in_hour_excludes,
        COUNT(1) AS swaps_in_hour,
        SUM(
            CASE
                WHEN exclude_from_pricing = FALSE THEN amount
            END
        ) total_amount_excludes,
        SUM(amount) AS total_amount
    FROM
        combo_3
    GROUP BY
        block_hour,
        block_hour :: DATE,
        currency,
        project_name,
        dex
),
FINAL AS (
    SELECT
        A.block_hour,
        A.currency,
        A.project_name,
        MIN(min_price_usd_hour) AS min_price_usd_hour,
        MAX(max_price_usd_hour) AS max_price_usd_hour,
        MAX(max_price_usd_hour) - MIN(min_price_usd_hour) AS volatility_measure,
        SUM(swaps_in_hour) AS swaps_in_hour,
        SUM(total_amount) AS volume_in_hour,
        SUM(
            avg_price_usd_hour_excludes
        ) AS price
    FROM
        (
            SELECT
                block_hour,
                block_date,
                currency,
                project_name,
                dex,
                avg_price_usd_hour_excludes,
                min_price_usd_hour,
                max_price_usd_hour,
                volatility_measure,
                swaps_in_hour,
                total_amount
            FROM
                final_dex
        ) A
    GROUP BY
        A.block_hour,
        A.currency,
        A.project_name
)

{% if is_incremental() %},
not_in_final AS (
    SELECT
        DATEADD(
            'HOUR',
            1,
            block_hour
        ) block_hour,
        currency,
        project_name,
        0 AS min_price_usd_hour,
        0 AS max_price_usd_hour,
        0 AS volatility_measure,
        0 AS swaps_in_hour,
        0 AS volume_in_hour,
        price_usd
    FROM
        {{ this }}
    WHERE
        currency NOT IN(
            SELECT
                DISTINCT currency
            FROM
                FINAL
        )
)
{% endif %},
fill_in_the_blanks_temp AS (
    SELECT
        A.hour AS block_hour,
        b.currency,
        b.project_name,
        C.price,
        C.min_price_usd_hour,
        C.max_price_usd_hour,
        C.volatility_measure,
        C.swaps_in_hour,
        C.volume_in_hour
    FROM
        (
            SELECT
                date_hour AS HOUR
            FROM
                {{ source(
                    'crosschain',
                    'dim_date_hours'
                ) }} A

{% if is_incremental() %}
WHERE
    HOUR > (
        SELECT
            MAX(block_hour)
        FROM
            {{ this }}
    )
    AND HOUR <= (
        SELECT
            max_date
        FROM
            swap_range
    )
{% else %}
    JOIN swap_range b
    ON A.hour BETWEEN b.min_date
    AND max_date
{% endif %}
) A
CROSS JOIN (
    SELECT
        DISTINCT currency,
        project_name
    FROM
        FINAL

{% if is_incremental() %}
UNION
SELECT
    DISTINCT currency,
    project_name
FROM
    {{ this }}
{% endif %}
) b
LEFT JOIN (
    SELECT
        *
    FROM
        FINAL

{% if is_incremental() %}
UNION ALL
SELECT
    *
FROM
    not_in_final
{% endif %}
) C
ON A.hour = C.block_hour
AND b.currency = C.currency
)
SELECT
    block_hour,
    currency,
    project_name,
    LAST_VALUE(
        price ignore nulls
    ) over(
        PARTITION BY currency
        ORDER BY
            block_hour ASC rows unbounded preceding
    ) AS price_usd,
    min_price_usd_hour,
    max_price_usd_hour,
    volatility_measure,
    swaps_in_hour,
    volume_in_hour * price AS volume_usd_in_hour
FROM
    fill_in_the_blanks_temp
WHERE
    project_name IN (
        'axlUSDC',
        'axlWBTC',
        'gWETH',
        'MARBLE',
        'axlFRAX',
        'gUSDC',
        'axlUSDT',
        'ASVT',
        'axlDAI',
        'axlWETH',
        'RAW',
        'gDAI',
        'ION'
    ) qualify(LAST_VALUE(price ignore nulls) over(PARTITION BY currency
ORDER BY
    block_hour ASC rows unbounded preceding)) IS NOT NULL
