{{ config (
    materialized = 'view',
    tags = ['core']
) }}

{% set model = this.identifier.split("_") [-1] %}
{{ streamline_external_table_FR_query(
    model = 'transactions',
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER)",
    partition_name = "_partition_by_block_id",
    unique_key = "COALESCE( block_number, DATA: tx_responses :height )"
) }}
