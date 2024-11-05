-- depends_on: {{ ref('bronze__streamline_balances') }}
{{ config (
    materialized = "incremental",
    unique_key = ["block_number","address"],
    cluster_by = "ROUND(block_number, -3)",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number,address)",
    enabled = false
) }}

WITH base AS (

    SELECT
        REPLACE(
            VALUE :metadata :request :url,
            '{service}/{Authentication}/cosmos/bank/v1beta1/balances/'
        ) AS url,
        CHARINDEX(
            '?',
            url
        ) ch_url,
        COALESCE(
            VALUE :ADDRESS :: STRING,
            LEFT(
                url,
                ch_url - 1
            )
        ) AS address,
        COALESCE(
            VALUE :BLOCK_NUMBER :: INT,
            VALUE :metadata :request :headers :"x-cosmos-block-height" :: INT
        ) AS block_number,
        file_name,
        inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_balances') }}
WHERE
    inserted_timestamp >= (
        SELECT
            COALESCE(MAX(modified_timestamp), '2000-01-01' :: datetime) modified_timestamp
        FROM
            {{ this }})
        {% else %}
            {{ ref('bronze__streamline_FR_balances') }}
        {% endif %}
    )
SELECT
    address,
    block_number,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number','address']
    ) }} AS complete_balances_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    file_name,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base qualify(ROW_NUMBER() over (PARTITION BY block_number, address
ORDER BY
    inserted_timestamp DESC)) = 1
