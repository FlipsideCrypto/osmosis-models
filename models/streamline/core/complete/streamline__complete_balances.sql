-- depends_on: {{ ref('bronze__streamline_balances') }}
{{ config (
    materialized = "incremental",
    unique_key = ["block_number","address"],
    cluster_by = "ROUND(block_number, -3)",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number,address)"
) }}

WITH base AS (

    SELECT
        REPLACE(
            REPLACE(
                VALUE :metadata :request :url,
                '{service}/{Authentication}/cosmos/bank/v1beta1/balances/'
            ),
            '?pagination.limit=1000'
        ) AS address,
        VALUE :metadata :request :headers :"x-cosmos-block-height" :: INT AS block_number,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_balances') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE(MAX(_inserted_timestamp), '2000-01-01' :: datetime) _inserted_timestamp
        FROM
            {{ this }})
        {% else %}
            {{ ref('bronze__streamline_FR_balances') }}
        {% endif %}
    )
SELECT
    address,
    block_number,
    _inserted_timestamp
FROM
    base qualify(ROW_NUMBER() over (PARTITION BY block_number, address
ORDER BY
    _inserted_timestamp DESC)) = 1
