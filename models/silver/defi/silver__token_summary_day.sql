{{ config(
    materialized = 'incremental',
    unique_key = ["currency","block_date"],
    incremental_strategy = 'merge',
    cluster_by = ['block_date'],
    tags = ['noncore']
) }}

WITH last_block_of_day AS (

    SELECT
        block_timestamp :: DATE AS block_date,
        MAX(block_id) AS block_id
    FROM
        {{ ref('silver__blocks') }}
    WHERE
        block_id >= 2300000
    GROUP BY
        block_date
)
SELECT
    blc.block_id,
    blc.block_date,
    token_0_denom AS currency,
    NULL AS market_Cap,
    AVG(
        twap
    ) price,
    AVG(twap_USD) price_usd,
    MAX(liquidity) liquidity,
    MAX(liquidity_usd) liquidity_usd,
    SUM(volume) volume,
    SUM(volume_usd) volume_usd,
    MAX(_inserted_timestamp) AS _inserted_timestamp
FROM
    last_block_of_day blc
    JOIN {{ ref('silver__pool_summary_hour') }} A
    ON blc.block_date = A.block_timestamp :: DATE

{% if is_incremental() %}
WHERE
    block_date >= (
        SELECT
            MAX(
                block_date
            )
        FROM
            {{ this }}
    ) - INTERVAL '7 days'
{% endif %}
GROUP BY
    blc.block_id,
    blc.block_Date,
    token_0_denom
