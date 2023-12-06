{{ config(
    materialized = 'view',
    tags = ['noncore']
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    msg_group AS msg_index,
    msg_type,
    msg,
    COALESCE(
        tx_body_msgs_id,
        {{ dbt_utils.generate_surrogate_key(
            ['_unique_key']
        ) }}
    ) AS fact_tx_body_msgs_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__tx_body_msgs') }}
