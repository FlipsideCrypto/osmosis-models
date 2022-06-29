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

vote_options AS (
    SELECT
        tx_id,
        msg_index,
        CASE
            WHEN attribute_value :: STRING = 'VOTE_OPTION_YES' THEN 1
            WHEN attribute_value :: STRING = 'VOTE_OPTION_ABSTAIN' THEN 2
            WHEN attribute_value :: STRING = 'VOTE_OPTION_NO' THEN 3
            WHEN attribute_value :: STRING = 'VOTE_OPTION_NO_WITH_VETO' THEN 4
            ELSE TRY_PARSE_JSON(attribute_value) :option
        END AS vote_option,
        TRY_PARSE_JSON(attribute_value) :weight :: FLOAT AS vote_weight
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        msg_type = 'proposal_vote'
        AND attribute_key = 'option'

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
),
proposal_id AS (
    SELECT
        tx_id,
        msg_index,
        attribute_value AS proposal_id
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        msg_type = 'proposal_vote'
        AND attribute_key = 'proposal_id'

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
),
voter AS (
    SELECT
        tx_id,
        msg_index,
        attribute_value AS voter
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        attribute_key = 'sender'

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
    o.tx_id,
    tx_status,
    v.voter,
    p.proposal_id,
    vote_option,
    vote_weight,
    _inserted_timestamp,
    concat_ws(
        '-',
        o.tx_id,
        p.proposal_id,
        v.voter
    ) AS _unique_key
FROM
    vote_options o
    LEFT OUTER JOIN proposal_id p
    ON o.tx_id = p.tx_id
    AND o.msg_index = p.msg_index
    LEFT OUTER JOIN voter v
    ON o.tx_id = v.tx_id
    AND o.msg_index = v.msg_index - 1
    LEFT OUTER JOIN {{ ref('silver__transactions') }}
    t
    ON o.tx_id = t.tx_id

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            max_date
    )
{% endif %}
