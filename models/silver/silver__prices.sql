{{ config(
    materialized = 'view',
) }}

SELECT
    recorded_at,
    symbol,
    price,
    total_supply,
    volume_24h,
    provider
FROM
    {{ source(
        'shared',
        'prices_v2'
    ) }}
WHERE
    (
        provider = 'coinmarketcap'
        AND (
            asset_id = '1281'
            OR asset_id = '7431'
            OR asset_id = '7281'
            OR asset_id = '3874'
            OR asset_id = '2643'
            OR asset_id = '3635'
            OR asset_id = '3026'
            OR asset_id = '12008'
            OR asset_id = '4679'
            OR asset_id = '707'
            OR asset_id = '3794'
            OR asset_id = '14973'
            OR asset_id = '2303'
            OR asset_id = '2659'
            OR asset_id = '2909'
            OR asset_id = '16214'
            OR asset_id = '1191'
            OR asset_id = '8905'
            OR asset_id = '13314'
            OR asset_id = '19899'
            OR asset_id = '18051'
            OR asset_id = '8279'
            OR asset_id = '17338'
            OR asset_id = '3773'
            OR asset_id = '4315'
            OR asset_id = '5604'
            OR asset_id = '9908'
            OR asset_id = '8996'
            OR asset_id = '5835'
            OR asset_id = '2620'
            OR asset_id = '4263'
            OR asset_id = '17208'
            OR asset_id = '9480'
        )
    )
    OR (
        provider = 'coingecko'
        AND (
            asset_id = 'osmosis'
            OR asset_id = 'pstake-finance'
            OR asset_id = 'e-money-eur'
            OR asset_id = 'juno-network'
            OR asset_id = 'terra-luna'
            OR asset_id = 'rac'
            OR asset_id = 'cerberus-2'
            OR asset_id = 'hash-token'
            OR asset_id = 'sommelier'
            OR asset_id = 'assetmantle'
            OR asset_id = 'microtick'
            OR asset_id = 'regen'
            OR asset_id = 'galaxer'
            OR asset_id = 'bootleg-nft'
            OR asset_id = 'terrausd'
            OR asset_id = 'umee'
            OR asset_id = 'cmdx'
            OR asset_id = 'terra-krw'
            OR asset_id = 'cheqd-network'
            OR asset_id = 'neta'
        )
    )
