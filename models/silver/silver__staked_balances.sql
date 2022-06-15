{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp'],
) }}

WITH all_staked AS (
    SELECT 
        block_id, 
        block_timestamp, 
        delegator_address AS address,
        amount, 
        currency, 
        CASE 
            WHEN currency LIKE 'gamm/pool/%' THEN 18
            ELSE raw_metadata[1]:exponent
        END AS decimal,
        _ingested_at
  
    FROM {{ ref('silver__staking') }} s
    
    LEFT OUTER JOIN {{ ref('silver__asset_metadata') }} a
    ON s.currency = a.address
  
    WHERE action = 'delegate'

    {% if is_incremental() %}
    AND _ingested_at :: DATE >= CURRENT_DATE -2
    {% endif %}
        
    UNION ALL 
  
    SELECT 
        block_id, 
        block_timestamp,  
        delegator_address AS address,
        -amount, 
        currency, 
        CASE 
            WHEN currency LIKE 'gamm/pool/%' THEN 18
            ELSE raw_metadata[1]:exponent
        END AS decimal,
        _ingested_at
  
    FROM {{ ref('silver__staking') }} s
    
    LEFT OUTER JOIN {{ ref('silver__asset_metadata') }} a
    ON s.currency = a.address
  
    WHERE action = 'undelegate'  

    {% if is_incremental() %}
    AND _ingested_at :: DATE >= CURRENT_DATE -2
    {% endif %}

) 

SELECT 
    block_id, 
    block_timestamp, 
    'staked' AS balance_type, 
    address, 
    currency, 
    decimal, 
    SUM(amount) OVER( PARTITION BY address,
    currency 
    ORDER BY block_timestamp ASC ROWS UNBOUNDED PRECEDING
    ) AS balance, 
    _ingested_at AS _inserted_timestamp
FROM all_staked