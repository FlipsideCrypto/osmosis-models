{{ config(
    materialized = 'incremental',
    unique_key = ['block_id', 'address', 'currency'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    tags = ['balances']
) }}
-- depends_on: {{ ref('bronze__streamline_balances') }}
WITH base AS (

    SELECT
        bal.block_id,
        bal.address,
        b.value :denom :: STRING AS currency,
        b.value :amount :: INT AS amount,
        TO_TIMESTAMP_NTZ(
            SUBSTR(SPLIT_PART(metadata$filename, '/', 4), 1, 10) :: NUMBER,
            0
        ) AS _inserted_timestamp
    FROM
        {{ source(
            'bronze_streamline',
            'balances_api'
        ) }}
        bal,
        LATERAL FLATTEN (
            input => balances,
            outer => TRUE
        ) b

{% if is_incremental() %}
WHERE
    0 = 1
{% endif %}
),
sl2 AS (
    SELECT
        REPLACE(
            A.value :metadata :request :url,
            '{service}/{Authentication}/cosmos/bank/v1beta1/balances/'
        ) AS url,
        CHARINDEX(
            '?',
            url
        ) ch_url,
        COALESCE(
            A.value :ADDRESS :: STRING,
            LEFT(
                url,
                ch_url - 1
            )
        ) AS address,
        COALESCE(
            A.value :BLOCK_NUMBER :: INT,
            A.value :metadata :request :headers :"x-cosmos-block-height" :: INT
        ) AS block_id,
        b.value :denom :: STRING AS currency,
        b.value :amount :: INT AS amount,
        inserted_timestamp AS _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_balances') }}
{% else %}
    {{ ref('bronze__streamline_FR_balances') }}
{% endif %}

A,
LATERAL FLATTEN (
    input => DATA: balances,
    outer => TRUE
) b

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
combo AS (
    SELECT
        block_id,
        address,
        currency,
        amount,
        _inserted_timestamp
    FROM
        base
    WHERE
        currency IS NOT NULL
    UNION ALL
    SELECT
        block_id,
        address,
        currency,
        amount,
        _inserted_timestamp
    FROM
        sl2
    WHERE
        currency IS NOT NULL
)
SELECT
    block_id,
    address,
    currency,
    amount,
    _inserted_timestamp
FROM
    combo qualify(ROW_NUMBER() over(PARTITION BY block_id, address, currency
ORDER BY
    _inserted_timestamp DESC)) = 1
