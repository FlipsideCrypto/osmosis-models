{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(tx_id,voter)",
    tags = ['noncore']
) }}

WITH base_tx AS (

    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        tx_body :memo :: STRING AS memo,
        tx_body :messages AS messages,
        _inserted_timestamp
    FROM
        {{ ref('silver__transactions') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
),
memo_text AS (
    SELECT
        tx_id,
        memo
    FROM
        base_tx
    WHERE
        memo IS NOT NULL
),
weighted_votes AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        messages,
        path :: STRING AS _path,
        _inserted_timestamp
    FROM
        base_tx,
        LATERAL FLATTEN (
            input => messages,
            recursive => TRUE
        ) b
    WHERE
        key = '@type'
        AND VALUE :: STRING = '/cosmos.gov.v1beta1.MsgVoteWeighted'
),
pre_final AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
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
        base_tx,
        LATERAL FLATTEN (
            input => messages,
            recursive => TRUE
        ) b
    WHERE
        key = '@type'
        AND VALUE :: STRING = '/cosmos.gov.v1beta1.MsgVote'
    UNION ALL
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
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
    FROM
        weighted_votes,
        LATERAL FLATTEN (
            input => messages
        ) b,
        LATERAL FLATTEN (
            input => b.value :options
        ) o
)
SELECT
    block_id,
    block_timestamp,
    m.tx_id,
    tx_succeeded,
    _path,
    proposal_id,
    voter,
    vote_option,
    vote_weight,
    memo,
    _inserted_timestamp,
    _unique_key,
    {{ dbt_utils.generate_surrogate_key(
        ['p.tx_id','proposal_id','voter','vote_option']
    ) }} AS governance_votes_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final p
    LEFT OUTER JOIN memo_text m
    ON p.tx_id = m.tx_id qualify ROW_NUMBER() over (
        PARTITION BY _unique_key
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
