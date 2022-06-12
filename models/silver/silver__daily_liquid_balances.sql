{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, address, currency)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['date'],
) }}

WITH recent AS (
    SELECT 
        block_id, 
        date, 
        address, 
        balance, 
        currency, 
        decimal,
        _inserted_timestamp 
    FROM {{ this }}
  
    WHERE date = (
        SELECT 
            DATEADD('day', -1, MAX(date))
      
        FROM {{ this }}
    )

), 

new AS (
    SELECT 
        block_id, 
        block_timestamp :: date AS date, 
        address, 
        balance, 
        currency, 
        decimal, 
        1 AS RANK, 
        _inserted_timestamp
    FROM 
        {{ ref('silver__liquid_balances') }}
    WHERE block_timestamp :: date >= (
        SELECT
            DATEADD('day', -1, MAX(DATE))
        FROM 
            {{ this }}
        ) 
        
    qualify(ROW_NUMBER() over (PARTITION BY block_timestamp :: date, address, currency
        ORDER BY 
            block_timestamp DESC)) = 1
    
), 

incremental AS (
    SELECT
        block_id, 
        date, 
        address, 
        balance, 
        currency, 
        decimal, 
        _inserted_timestamp
    FROM 
        (
            SELECT
                block_id, 
                date, 
                address, 
                balance, 
                currency, 
                decimal, 
                2 AS RANK, 
                _inserted_timestamp
            FROM 
                recent
            
            UNION

            SELECT
                block_id, 
                date, 
                address, 
                balance, 
                currency, 
                decimal, 
                1 AS RANK, 
                _inserted_timestamp
            FROM 
                new
        )

    qualify(ROW_NUMBER() over (PARTITION BY date, address, currency
        ORDER BY 
            date DESC)) = 1
    
), 

base AS (

    {% if is_incremental() %}

    SELECT 
        block_id, 
        date AS block_timestamp,
        address, 
        balance, 
        currency, 
        decimal, 
        _inserted_timestamp
    FROM
        incremental
    
    {% else %}
    
     SELECT 
        block_id, 
        block_timestamp,
        address, 
        balance, 
        currency, 
        decimal, 
        _inserted_timestamp
    FROM
        {{ ref('silver__liquid_balances') }}

    {% endif %}
), 

address_ranges AS (
    SELECT
        address, 
        currency, 
        MIN(
            block_timestamp :: date
        ) AS min_block_date, 
        MAX (
            block_timestamp :: date
        ) AS max_block_date
    FROM 
        base
    GROUP BY 
        address, 
        currency
), 

ddate AS (
    SELECT
        hour :: date AS date
    FROM 
        {{ source(
            'shared2', 
            'hours'
        ) }}
    GROUP BY date
), 

all_dates AS (
    SELECT 
        d.date, 
        a.address, 
        a.currency
    FROM 
        ddate d
    
    LEFT JOIN address_ranges a
    ON d.date BETWEEN a.min_block_date AND a.max_block_date

    WHERE 
        a.address IS NOT NULL 
), 

osmosis_balances AS (
    SELECT 
        block_id, 
        block_timestamp,
        address, 
        balance, 
        currency, 
        decimal, 
        _inserted_timestamp
    FROM
        base
    
    qualify(ROW_NUMBER() over (PARTITION BY block_timestamp :: date, address, currency
        ORDER BY 
            block_timestamp DESC)) = 1
), 

balance_temp AS (
    SELECT
        b.block_id, 
        d.date, 
        d.address, 
        b.balance, 
        d.currency,
        b.decimal, 
        _inserted_timestamp

    FROM 
        all_dates d 
    
    LEFT JOIN osmosis_balances b
    ON d.date = b.block_timestamp :: date
    AND d.address = b.address
    AND d.currency = b.currency
)

SELECT
    block_id, 
    date, 
    'liquid' AS balance_type, 
    address, 
    LAST_VALUE(
        balance ignore nulls
    ) over(
    PARTITION BY address,
    currency
    ORDER BY
      DATE ASC rows unbounded preceding
    )  AS balance, 
    currency, 
    decimal, 
    _inserted_timestamp
FROM 
    balance_temp

