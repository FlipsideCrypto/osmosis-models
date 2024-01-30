{{ config(
    materialized = 'view',
    tags = ['noncore']
) }}

SELECT
    blockchain,
    address,
    creator,
    label_type,
    label_subtype,
    label,
    project_name,
    raw_metadata,
    COALESCE(
        asset_metadata_id,
        {{ dbt_utils.generate_surrogate_key(
            ['address']
        ) }}
    ) AS dim_labels_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__asset_metadata') }}
UNION ALL
SELECT
    blockchain,
    address,
    creator,
    label_type,
    label_subtype,
    label,
    project_name,
    raw_metadata,
    COALESCE(
        validator_metadata_id,
        {{ dbt_utils.generate_surrogate_key(
            [' _unique_key ']
        ) }}
    ) AS dim_labels_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__validator_metadata') }}
UNION ALL
SELECT
    blockchain,
    address,
    creator,
    label_type,
    label_subtype,
    address_name AS label,
    project_name,
    NULL AS raw_metadata,
    labels_combined_id AS dim_labels_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref(
        'silver__croschain_labels'
    ) }}
WHERE
    label_subtype <> 'validator'
