{{ config(
    materialized = 'view',
      meta={
        'database_tags':{
            'table': {
                'PURPOSE': 'GOVERNANCE'
            }
        }
      }
) }}

SELECT 
    block_id, 
    block_timestamp, 
    tx_id, 
    tx_succeeded,
    depositor, 
    proposal_id, 
    amount, 
    currency, 
    decimal
FROM {{ ref('silver__governance_proposal_deposits') }}