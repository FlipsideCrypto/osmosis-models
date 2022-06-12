{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, address, balance, currency)",
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

    qualify(ROW_NUMBER() over(PARTITION BY block_id, block_timestamp, currency, amount
    ORDER BY
    _ingested_at DESC)) = 1
        
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

    qualify(ROW_NUMBER() over(PARTITION BY block_id, block_timestamp, currency, amount
    ORDER BY
    _ingested_at DESC)) = 1
) 

SELECT 
    block_id, 
    block_timestamp, 
    'staked' AS balance_type, 
    address, 
    SUM(amount) OVER( ORDER BY block_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as balance, 
    currency, 
    decimal, 
    _ingested_at
FROM all_staked