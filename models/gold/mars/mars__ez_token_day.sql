{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEFI, PRICES' }} },
    tags = ['noncore'],
    enabled = false
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
    volume_usd,
    COALESCE(
        token_summary_day_id,
        {{ dbt_utils.generate_surrogate_key(
            ['currency', 'block_date']
        ) }}
    ) AS ez_token_day_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__token_summary_day') }}
WHERE
    currency = 'ibc/573FCD90FACEE750F55A8864EF7D38265F07E5A9273FA0E8DAFD39951332B580'
