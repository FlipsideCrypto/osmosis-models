{{ config (
    materialized = "ephemeral"
) }}

SELECT
    COALESCE(MIN(block_id), 0) AS block_number
FROM
    {{ ref("silver__blocks") }}
WHERE
    block_timestamp >= DATEADD('hour', -72, TRUNCATE(SYSDATE(), 'HOUR'))
    AND block_timestamp < DATEADD('hour', -71, TRUNCATE(SYSDATE(), 'HOUR'))