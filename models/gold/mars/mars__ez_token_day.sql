{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEFI, PRICES' }} },
    tags = ['noncore']
) }}

SELECT
    block_id,
    block_date,
    currency,
    'MARS' AS symbol,
    price,
    price_usd,
    liquidity,
    liquidity_usd,
    volume,
    volume_usd
FROM
    {{ ref('silver__token_summary_day') }}
WHERE
    currency = 'ibc/573FCD90FACEE750F55A8864EF7D38265F07E5A9273FA0E8DAFD39951332B580'
