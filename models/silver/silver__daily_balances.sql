{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', date, address, balance_type, currency)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['date'],
) }}

WITH recent AS (
    SELECT  
        date, 
        balance_type, 
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
        block_timestamp :: date AS date, 
        balance_type, 
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
        
    qualify(ROW_NUMBER() over (PARTITION BY block_timestamp :: date, address, balance_type, currency
        ORDER BY 
            _inserted_timestamp DESC)) = 1

    UNION ALL
    
    SELECT 
        block_timestamp :: date AS date, 
        balance_type, 
        address, 
        balance, 
        currency, 
        decimal, 
        1 AS RANK, 
        _inserted_timestamp
    FROM 
        {{ ref('silver__staked_balances') }}

    WHERE block_timestamp :: date >= (
        SELECT
            DATEADD('day', -1, MAX(DATE))
        FROM 
            {{ this }}
        ) 
        
    qualify(ROW_NUMBER() over (PARTITION BY block_timestamp :: date, address, balance_type, currency
        ORDER BY 
            block_timestamp DESC)) = 1
    
), 

incremental AS (
    SELECT
        date, 
        balance_type, 
        address, 
        balance, 
        currency, 
        decimal, 
        _inserted_timestamp
    FROM 
        (
            SELECT
                date, 
                balance_type, 
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
                date, 
                balance_type, 
                address, 
                balance, 
                currency, 
                decimal, 
                1 AS RANK, 
                _inserted_timestamp
            FROM 
                new
        )

    qualify(ROW_NUMBER() over (PARTITION BY date, address, balance_type, currency
        ORDER BY 
            RANK ASC)) = 1
    
), 

base AS (

    {% if is_incremental() %}

    SELECT 
        date AS block_timestamp,
        balance_type, 
        address, 
        balance, 
        currency, 
        decimal, 
        _inserted_timestamp
    FROM
        incremental
    
    {% else %}
    
    SELECT 
        block_timestamp,
        balance_type, 
        address, 
        balance, 
        currency, 
        decimal, 
        _inserted_timestamp
    FROM
        {{ ref('silver__liquid_balances') }}

    UNION ALL 

    SELECT 
        block_timestamp,
        balance_type, 
        address, 
        balance, 
        currency, 
        decimal, 
        _inserted_timestamp
    FROM
        {{ ref('silver__staked_balances') }}

    {% endif %}
), 

address_ranges AS (
    SELECT
        address, 
        balance_type, 
        currency, 
        decimal, 
        MIN(
            block_timestamp :: date
        ) AS min_block_date, 
        MAX (
            CURRENT_TIMESTAMP :: date
        ) AS max_block_date
    FROM 
        base
    GROUP BY 
        address, 
        balance_type, 
        currency, 
        decimal
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
        a.balance_type, 
        a.address, 
        a.currency, 
        a.decimal
    FROM 
        ddate d
    
    LEFT JOIN address_ranges a
    ON d.date 
    BETWEEN a.min_block_date 
    AND a.max_block_date

    WHERE 
        a.address IS NOT NULL 
), 

osmosis_balances AS (
    SELECT 
        block_timestamp,
        balance_type, 
        address, 
        balance, 
        currency, 
        decimal, 
        _inserted_timestamp
    FROM
        base
    
    qualify(ROW_NUMBER() over (PARTITION BY block_timestamp :: date, address, balance_type, currency
        ORDER BY 
            block_timestamp DESC)) = 1
), 

balance_temp AS (
    SELECT
        d.date, 
        d.balance_type, 
        d.address, 
        b.balance, 
        d.currency,
        d.decimal, 
        _inserted_timestamp

    FROM 
        all_dates d 
    
    LEFT JOIN osmosis_balances b
    ON d.date = b.block_timestamp :: date
    AND d.address = b.address
    AND d.currency = b.currency
    AND d.balance_type = b.balance_type
)

SELECT
    date, 
    balance_type, 
    address, 
    currency, 
    decimal, 
    LAST_VALUE(
        balance ignore nulls
    ) over(
    PARTITION BY address,
    currency, 
    balance_type
    ORDER BY
      DATE ASC rows unbounded preceding
    )  AS balance, 
    _inserted_timestamp
FROM 
    balance_temp