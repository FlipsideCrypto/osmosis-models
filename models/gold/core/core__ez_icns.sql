{{ config(
    materialized = 'view',
    tags = ['noncore']
) }}

SELECT
    blockchain,
    creator,
    address,
    label,
    project_name,
    start_date,
    end_date,
    COALESCE(
        crosschain_icns_id,
        {{ dbt_utils.generate_surrogate_key(
            ['address']
        ) }}
    ) AS ez_icns_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref(
        'silver__croschain_icns'
    ) }}
