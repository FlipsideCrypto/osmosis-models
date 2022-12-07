{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH

{% if is_incremental() %}
max_date AS (

    SELECT
        MAX(
            _inserted_timestamp
        ) _inserted_timestamp
    FROM
        {{ this }}
),
{% endif %}

weighted_votes AS (
    SELECT 
        block_id, 
        block_timestamp, 
        blockchain, 
        chain_id, 
        tx_id, 
        tx_status, 
        tx_body, 
        path :: STRING AS _path, 
        _inserted_timestamp
    FROM 
        {{ ref('silver__transactions') }}, 
    LATERAL FLATTEN (input => tx_body :messages, recursive => TRUE ) b 
    WHERE 
        key = '@type' 
        AND VALUE :: STRING = '/cosmos.gov.v1beta1.MsgVoteWeighted'

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
    blockchain, 
    chain_id, 
    tx_id, 
    tx_status, 
    path :: STRING AS _path, 
    this :proposal_id :: STRING AS proposal_id, 
    this :voter :: STRING AS voter, 
    CASE
        WHEN this :option :: STRING = 'VOTE_OPTION_YES' THEN 1
        WHEN this :option :: STRING = 'VOTE_OPTION_ABSTAIN' THEN 2
        WHEN this :option :: STRING = 'VOTE_OPTION_NO' THEN 3
        WHEN this :option :: STRING = 'VOTE_OPTION_NO_WITH_VETO' THEN 4
        ELSE this :option
    END AS vote_option,
    1.000 AS vote_weight, 
    _inserted_timestamp, 
    concat_ws(
        '-',
        tx_id,
        proposal_id,
        voter, 
        vote_option, 
        _path
    ) AS _unique_key
FROM 
    {{ ref('silver__transactions') }}, 
LATERAL FLATTEN (input => tx_body :messages, recursive => TRUE ) b
WHERE 
    key = '@type' 
    AND VALUE :: STRING = '/cosmos.gov.v1beta1.MsgVote'

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
    blockchain, 
    chain_id, 
    tx_id, 
    tx_status, 
    _path, 
    b.value :proposal_id :: NUMBER AS proposal_id, 
    b.value :voter :: STRING AS voter, 
    CASE
        WHEN o.value :option :: STRING = 'VOTE_OPTION_YES' THEN 1
        WHEN o.value :option :: STRING = 'VOTE_OPTION_ABSTAIN' THEN 2
        WHEN o.value :option :: STRING = 'VOTE_OPTION_NO' THEN 3
        WHEN o.value :option :: STRING = 'VOTE_OPTION_NO_WITH_VETO' THEN 4
        ELSE o.value :option
    END AS vote_option,
    o.value :weight :: FLOAT AS vote_weight, 
    _inserted_timestamp, 
    concat_ws(
        '-',
        tx_id,
        proposal_id,
        voter, 
        vote_option, 
        _path
    ) AS _unique_key
FROM weighted_votes, 
LATERAL FLATTEN (input => tx_body :messages) b, 
LATERAL FLATTEN (input => b.value :options) o