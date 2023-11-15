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
    raw_metadata
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
    raw_metadata
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
    NULL AS raw_metadata
FROM
    {{ ref(
        'silver__croschain_labels'
    ) }}
WHERE
    label_subtype <> 'validator'
