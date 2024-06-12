{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['recorded_hour::DATE'],
    tags = ['noncore']
) }}

WITH date_hours AS (

    SELECT
        date_hour
    FROM
        {{ source (
            'crosschain',
            'dim_date_hours'
        ) }}
    WHERE
        date_hour >= '2020-04-10'
        AND date_hour <= (
            SELECT
                MAX(recorded_hour)
            FROM
                {{ source(
                    'crosschain_silver',
                    'token_prices_coingecko'
                ) }}
        )

{% if is_incremental() %}
AND date_hour > (
    SELECT
        MAX(recorded_hour)
    FROM
        {{ this }}
)
{% endif %}
),
asset_metadata AS (
    SELECT
        id,
        symbol
    FROM
        {{ source(
            'crosschain_silver',
            'token_asset_metadata_coingecko'
        ) }}
    WHERE
        id IN (
            'pstake-finance',
            'e-money-eur',
            'juno-network',
            'terra-luna',
            'cerberus-2',
            'hash-token',
            'sommelier',
            'assetmantle',
            'microtick',
            'regen',
            'galaxer',
            'bootleg-nft',
            'terrausd',
            'umee',
            'cmdx',
            'terra-krw',
            'cheqd-network',
            'neta',
            'medibloc',
            'kujira',
            'likecoin',
            'dig-chain',
            'hope-galaxy',
            'wrapped-bitcoin',
            'comdex',
            'darcmatter-coin',
            'ixo',
            'osmosis',
            'persistence',
            'stakeeasy-bjuno',
            'stakeeasy-juno-derivative',
            'stride-staked-atom',
            'seasy',
            'cosmos',
            'crescent-network',
            'crypto-com-chain',
            'injective-protocol',
            'arable-protocol',
            'inter-stable-token',
            'weth',
            'usdx',
            'odin-protocol',
            'chihuahua-token',
            'agoric',
            'stargaze',
            'lum-network',
            'starname',
            'ki',
            'graviton',
            'e-money',
            'fetch-ai',
            'axelar',
            'racoon',
            'posthuman',
            'sentinel',
            'stride',
            'usk',
            'dai',
            'ion',
            'iris-network',
            'evmos',
            'desmos',
            'akash-network'
        )
    GROUP BY
        1,
        2
),
base_date_hours_symbols AS (
    SELECT
        date_hour,
        id,
        symbol
    FROM
        date_hours
        CROSS JOIN asset_metadata
),
base_legacy_prices AS (
    SELECT
        DATE_TRUNC(
            'hour',
            recorded_at
        ) AS recorded_hour,
        asset_id AS id,
        symbol,
        price AS CLOSE
    FROM
        {{ source(
            'crosschain_bronze',
            'legacy_prices'
        ) }}
    WHERE
        provider = 'coingecko'
        AND asset_id IN (
            'pstake-finance',
            'e-money-eur',
            'juno-network',
            'terra-luna',
            'cerberus-2',
            'hash-token',
            'sommelier',
            'assetmantle',
            'microtick',
            'regen',
            'galaxer',
            'bootleg-nft',
            'terrausd',
            'umee',
            'cmdx',
            'terra-krw',
            'cheqd-network',
            'neta',
            'medibloc',
            'kujira',
            'likecoin',
            'dig-chain',
            'hope-galaxy',
            'wrapped-bitcoin',
            'comdex',
            'darcmatter-coin',
            'ixo',
            'osmosis',
            'persistence',
            'stakeeasy-bjuno',
            'stakeeasy-juno-derivative',
            'stride-staked-atom',
            'seasy',
            'cosmos',
            'crescent-network',
            'crypto-com-chain',
            'injective-protocol',
            'arable-protocol',
            'inter-stable-token',
            'weth',
            'usdx',
            'odin-protocol',
            'chihuahua-token',
            'agoric',
            'stargaze',
            'lum-network',
            'starname',
            'ki',
            'graviton',
            'e-money',
            'fetch-ai',
            'axelar',
            'racoon',
            'posthuman',
            'sentinel',
            'stride',
            'usk',
            'dai',
            'ion',
            'iris-network',
            'evmos',
            'desmos',
            'akash-network'
        )
        AND MINUTE(recorded_at) = 59
        AND recorded_at :: DATE < '2022-07-20' -- use legacy data before this date

{% if is_incremental() %}
AND recorded_at > (
    SELECT
        MAX(recorded_hour)
    FROM
        {{ this }}
)
{% endif %}
),
base_prices AS (
    SELECT
        recorded_hour,
        p.id,
        m.symbol,
        p.close
    FROM
        {{ source(
            'crosschain_silver',
            'token_prices_coingecko'
        ) }}
        p
        LEFT OUTER JOIN asset_metadata m
        ON m.id = p.id
    WHERE
        p.id IN (
            'pstake-finance',
            'e-money-eur',
            'juno-network',
            'terra-luna',
            'cerberus-2',
            'hash-token',
            'sommelier',
            'assetmantle',
            'microtick',
            'regen',
            'galaxer',
            'bootleg-nft',
            'terrausd',
            'umee',
            'cmdx',
            'terra-krw',
            'cheqd-network',
            'neta',
            'medibloc',
            'kujira',
            'likecoin',
            'dig-chain',
            'hope-galaxy',
            'wrapped-bitcoin',
            'comdex',
            'darcmatter-coin',
            'ixo',
            'osmosis',
            'persistence',
            'stakeeasy-bjuno',
            'stakeeasy-juno-derivative',
            'stride-staked-atom',
            'seasy',
            'cosmos',
            'crescent-network',
            'crypto-com-chain',
            'injective-protocol',
            'arable-protocol',
            'inter-stable-token',
            'weth',
            'usdx',
            'odin-protocol',
            'chihuahua-token',
            'agoric',
            'stargaze',
            'lum-network',
            'starname',
            'ki',
            'graviton',
            'e-money',
            'fetch-ai',
            'axelar',
            'racoon',
            'posthuman',
            'sentinel',
            'stride',
            'usk',
            'dai',
            'ion',
            'iris-network',
            'evmos',
            'desmos',
            'akash-network'
        )
        AND recorded_hour :: DATE >= '2022-07-20'

{% if is_incremental() %}
AND recorded_hour > (
    SELECT
        MAX(recorded_hour)
    FROM
        {{ this }}
)
{% endif %}
),
prices AS (
    SELECT
        *
    FROM
        base_legacy_prices
    UNION
    SELECT
        *
    FROM
        base_prices
),
imputed_prices AS (
    SELECT
        d.*,
        p.close AS hourly_close,
        LAST_VALUE(
            p.close ignore nulls
        ) over (
            PARTITION BY d.symbol
            ORDER BY
                d.date_hour rows unbounded preceding
        ) AS imputed_close
    FROM
        base_date_hours_symbols d
        LEFT OUTER JOIN prices p
        ON p.recorded_hour = d.date_hour
        AND p.id = d.id
)
SELECT
    p.date_hour AS recorded_hour,
    p.id,
    CASE
        WHEN p.symbol IS NOT NULL THEN p.symbol
        ELSE CASE
            p.id
            WHEN 'cerberus-2' THEN 'CRBRUS'
            WHEN 'cheqd-network' THEN 'CHEQ'
            WHEN 'e-money-eur' THEN 'EEUR'
            WHEN 'juno-network' THEN 'JUNO'
            WHEN 'kujira' THEN 'KUJI'
            WHEN 'medibloc' THEN 'MED'
            WHEN 'microtick' THEN 'TICK'
            WHEN 'neta' THEN 'NETA'
            WHEN 'regen' THEN 'REGEN'
            WHEN 'sommelier' THEN 'SOMM'
            WHEN 'terra-luna' THEN 'LUNC'
            WHEN 'umee' THEN 'UMEE'
        END
    END AS symbol,
    COALESCE(
        p.hourly_close,
        p.imputed_close
    ) AS CLOSE,
    CASE
        WHEN p.hourly_close IS NULL THEN TRUE
        ELSE FALSE
    END AS imputed,
    concat_ws(
        '-',
        recorded_hour,
        symbol
    ) AS _unique_key,
    {{ dbt_utils.generate_surrogate_key(
        ['_unique_key']
    ) }} AS token_prices_coin_gecko_hourly_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    imputed_prices p
WHERE
    CLOSE IS NOT NULL
    AND symbol IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY recorded_hour, symbol
ORDER BY
    id) = 1)
