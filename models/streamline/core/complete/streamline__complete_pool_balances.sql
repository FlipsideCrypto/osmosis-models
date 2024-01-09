-- depends_on: {{ ref('bronze__streamline_pool_balances') }}
{{ config (
    materialized = "incremental",
    unique_key = "block_number",
    cluster_by = "ROUND(block_number, -3)",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)"
) }}

WITH base AS (

    SELECT
        VALUE :metadata :request :headers :"x-cosmos-block-height" :: INT AS block_number,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_pool_balances') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE(MAX(_inserted_timestamp), '2000-01-01' :: datetime) _inserted_timestamp
        FROM
            {{ this }})
        {% else %}
            {{ ref('bronze__streamline_FR_pool_balances') }}
        {% endif %}
    )
SELECT
    block_number,
    _inserted_timestamp
FROM
    base qualify(ROW_NUMBER() over (PARTITION BY block_number
ORDER BY
    _inserted_timestamp DESC)) = 1
