{{ config(
  materialized = 'incremental',
  unique_key = "proposal_id",
  incremental_strategy = 'delete+insert',
  full_refresh = false,
  tags = ['daily']
) }}

WITH valid_props AS (

  SELECT
    A.proposal_id,
    MAX(
      A._inserted_timestamp
    ) AS _inserted_timestamp
  FROM
    {{ ref('silver__governance_votes') }} A
    JOIN {{ ref('silver__governance_submit_proposal') }}
    b
    ON A.proposal_id = b.proposal_id
  GROUP BY
    A.proposal_id
),
base AS (
  SELECT
    proposal_id
  FROM
    valid_props
  WHERE
    _inserted_timestamp :: DATE > CURRENT_DATE -3
  UNION
  SELECT
    proposal_id
  FROM
    (
      SELECT
        proposal_id
      FROM
        valid_props

{% if is_incremental() %}
EXCEPT
SELECT
  proposal_id
FROM
  {{ this }}
{% endif %}
)
ORDER BY
  1
),
call AS (
  SELECT
    top 50 ethereum.streamline.udf_api(
      'GET',
      'https://lcd-osmosis.keplr.app/cosmos/gov/v1beta1/proposals/' || proposal_id :: INT :: STRING,{},{}
    ) AS resp,
    proposal_id,
    SYSDATE() AS _inserted_timestamp
  FROM
    base
)
SELECT
  proposal_id :: INT AS proposal_id,
  resp,
  _inserted_timestamp
FROM
  call
