{{ config(
    materialized = 'table',
    tags = ['daily']
) }}

SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    label_type,
    label_subtype,
    address_name,
    project_name,
    {{ dbt_utils.generate_surrogate_key(
        ['address']
    ) }} AS crosschain_labels_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ source(
        'crosschain',
        'dim_labels'
    ) }}
WHERE
    blockchain = 'osmosis'
