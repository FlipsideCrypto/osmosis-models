{{ config(
  materialized = 'incremental',
  unique_key = "tx_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['_ingested_at::DATE'],
) }}

WITH vote_options AS (
  SELECT 
      tx_id, 
      msg_index, 
      CASE 
        WHEN attribute_value::string = 'VOTE_OPTION_YES' THEN
            1
        WHEN attribute_value::string = 'VOTE_OPTION_ABSTAIN' THEN
            2
        WHEN attribute_value::string = 'VOTE_OPTION_NO' THEN
            3
        WHEN attribute_value::string = 'VOTE_OPTION_NO_WITH_VETO' THEN
            4
        ELSE
            TRY_PARSE_JSON(attribute_value):option 
      END AS vote_option, 
      TRY_PARSE_JSON(attribute_value):weight :: FLOAT AS vote_weight
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
        msg_index, 
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
    vote_option, 
    vote_weight, 
    _ingested_at
FROM vote_options o

LEFT OUTER JOIN proposal_id p
ON o.tx_id = p.tx_id AND o.msg_index = p.msg_index

LEFT OUTER JOIN voter v
ON o.tx_id = v.tx_id 

LEFT OUTER JOIN {{ ref('silver__transactions') }} t
ON o.tx_id = t.tx_id 

{% if is_incremental() %}
WHERE _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}