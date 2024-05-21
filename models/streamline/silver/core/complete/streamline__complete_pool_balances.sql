-- depends_on: {{ ref('bronze__streamline_pool_balances') }}
{{ config (
    materialized = "incremental",
    unique_key = "block_number",
    cluster_by = "ROUND(block_number, -3)",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)"
) }}

SELECT
    COALESCE(
        VALUE :BLOCK_NUMBER :: INT,
        VALUE :metadata :request :headers :"x-cosmos-block-height" :: INT
    ) AS block_number,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number']
    ) }} AS complete_pool_balances_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    file_name,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_pool_balances') }}
{# WHERE
inserted_timestamp >= (
    SELECT
        COALESCE(MAX(modified_timestamp), '2000-01-01' :: datetime) modified_timestamp
    FROM
        {{ this }}) #}
    {% else %}
        {{ ref('bronze__streamline_FR_pool_balances') }}
    {% endif %}

    qualify(ROW_NUMBER() over (PARTITION BY block_number
    ORDER BY
        inserted_timestamp DESC)) = 1
