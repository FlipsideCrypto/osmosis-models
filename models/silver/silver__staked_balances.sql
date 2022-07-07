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
        _inserted_timestamp
  
    FROM {{ ref('silver__staking') }} s
    
    LEFT OUTER JOIN {{ ref('silver__asset_metadata') }} a
    ON s.currency = a.address
  
    WHERE action = 'delegate'

    {% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        max_date
)
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
        _inserted_timestamp
  
    FROM {{ ref('silver__staking') }} s
    
    LEFT OUTER JOIN {{ ref('silver__asset_metadata') }} a
    ON s.currency = a.address
  
    WHERE action = 'undelegate'  

    {% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        max_date
)
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
    _inserted_timestamp
FROM all_staked