{{ config(
  materialized = 'incremental',
  unique_key = "tx_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['_ingested_at::DATE'],
) }}

WITH vote_options AS (
  SELECT 
      tx_id, 
      COALESCE(PARSE_JSON(attribute_value):option, 
                PARSE_JSON(attribute_value):vote_option_yes) AS v_option, 
      PARSE_JSON(attribute_value):weight :: INTEGER AS vote_weight
  FROM {{ ref('silver__msg_attributes') }}
  WHERE msg_type = 'proposal_vote'
  AND attribute_key = 'option'
 
  {% if is_incremental() %}
  AND _ingested_at :: DATE >= CURRENT_DATE - 2
  {% endif %}
), 

proposal_id AS (
    SELECT 
        tx_id, 
        attribute_value AS proposal_id 
    FROM {{ ref('silver__msg_attributes') }}
    WHERE msg_type = 'proposal_vote' 
    AND attribute_key = 'proposal_id'

    {% if is_incremental() %}
    AND _ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
), 

voter AS (
    SELECT 
        tx_id, 
        split_part(attribute_value, '/', 0) as voter
    FROM {{ ref('silver__msg_attributes') }}
    WHERE attribute_key = 'acc_seq'

    {% if is_incremental() %}
    AND _ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
)

SELECT 
    block_id, 
    block_timestamp, 
    blockchain, 
    chain_id, 
    o.tx_id, 
    tx_status, 
    v.voter, 
    p.proposal_id, 
    CASE WHEN v_option = 1 THEN 'Yes'
         WHEN v_option = 2 THEN 'No'
         WHEN v_option = 3 THEN 'NoWithVeto'
         ELSE 'ABSTAIN' END AS vote_option, 
    vote_weight, 
    _ingested_at
FROM vote_options o

LEFT OUTER JOIN proposal_id p
ON o.tx_id = p.tx_id

LEFT OUTER JOIN voter v
ON o.tx_id = v.tx_id 

LEFT OUTER JOIN {{ ref('silver__transactions') }} t
ON o.tx_id = t.tx_id 

{% if is_incremental() %}
WITH _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}