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
    msg
FROM
    {{ ref('silver__tx_body_msgs') }}
