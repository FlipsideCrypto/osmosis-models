{{ config (
    materialized = 'view'
) }}

SELECT
    *
FROM
    {{ source(
        'bronze_streamline',
        'asset_metadata_api'
    ) }}
