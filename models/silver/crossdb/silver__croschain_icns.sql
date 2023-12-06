{{ config(
    materialized = 'table',
    tags = ['daily']
) }}

SELECT
    blockchain,
    creator,
    address,
    tag_name AS label,
    tag_type AS project_name,
    start_date,
    end_date,
    {{ dbt_utils.generate_surrogate_key(
        ['address']
    ) }} AS crosschain_icns_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ source(
        'crosschain',
        'dim_tags'
    ) }}
WHERE
    tag_type = 'ICNS'
