{{ config(
    materialized = 'view',
      meta={
        'database_tags':{
            'table': {
                'PURPOSE': 'STAKING'
            }
        }
      }
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    action,
    delegator_address,
    amount,
    currency,
    DECIMAL,
    validator_address,
    lock_id
FROM
    {{ ref('silver__superfluid_staking') }}
