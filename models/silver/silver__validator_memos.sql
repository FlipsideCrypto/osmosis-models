{{ config(
    materialized = 'incremental',
    unique_key = 'proposal_id',
    incremental_strategy = 'delete+insert'
) }}

WITH base AS (

    SELECT
        proposal_id,
        resp,
        _inserted_timestamp
    FROM
        {{ ref('bronze_api__validator_memos') }} A

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
)
SELECT
    proposal_id,
    i.value :validator_address :: STRING AS validator_address,
    i.value :memo :: STRING AS memo,
    i.value :vote :: STRING AS vote,
    i.value :voting_power :: FLOAT AS voting_power,
    i.value :version :: INT AS version,
    i.value :created_at :: datetime AS created_at,
    i.value :updated_at :: datetime AS updated_at,
    _inserted_timestamp
FROM
    base,
    LATERAL FLATTEN(
        input => resp :data
    ) i