{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'SWAPS' }} },
    tags = ['noncore']
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    trader,
    COALESCE(
        from_amount / pow(
            10,
            CASE
                WHEN from_amount LIKE 'gamm/pool/%' THEN 18
                ELSE f.decimal
            END
        ),
        from_amount
    ) AS from_amount,
    from_currency,
    f.project_name from_symbol,
    COALESCE(
        to_amount / pow(
            10,
            CASE
                WHEN to_amount LIKE 'gamm/pool/%' THEN 18
                ELSE t.decimal
            END
        ),
        to_amount
    ) AS to_amount,
    to_currency,
    t.project_name to_symbol,
    pool_ids
FROM
    {{ ref('silver__swaps') }} A
    LEFT JOIN {{ ref('silver__asset_metadata') }}
    f
    ON A.from_currency = f.address
    LEFT JOIN {{ ref('silver__asset_metadata') }}
    t
    ON A.to_currency = t.address
WHERE
    from_currency = 'ibc/573FCD90FACEE750F55A8864EF7D38265F07E5A9273FA0E8DAFD39951332B580'
    OR to_currency = 'ibc/573FCD90FACEE750F55A8864EF7D38265F07E5A9273FA0E8DAFD39951332B580'
