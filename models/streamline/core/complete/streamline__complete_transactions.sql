-- depends_on: {{ ref('bronze__streamline_transactions') }}
{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(id)"
) }}

SELECT
    id,
    COALESCE(
        block_number,
        DATA: tx_responses :height
    ) AS block_number,
    SUBSTR(
        metadata :request :url,
        CHARINDEX(
            'offset=',
            metadata :request :url
        ) + 7,
        99
    ) :: STRING AS pagination_offset,
    _inserted_timestamp
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_transactions') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__streamline_FR_transactions') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY id, pagination_offset
ORDER BY
    _inserted_timestamp DESC)) = 1
