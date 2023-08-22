{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STAKING' }}}
) }}

SELECT
    *
FROM
    {{ ref('silver__staking_rewards') }}
