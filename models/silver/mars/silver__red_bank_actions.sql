{{ config(
    materialized = 'incremental',
    unique_key = ["tx_id","msg_index"],
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE','action']
) }}

WITH mars_contracts AS (

    SELECT
        'mars-address-provider' contract_name,
        'osmo1g677w7mfvn78eeudzwylxzlyz69fsgumqrscj6tekhdvs8fye3asufmvxr' address
    UNION ALL
    SELECT
        'mars-incentives' contract_name,
        'osmo1nkahswfr8shg8rlxqwup0vgahp0dk4x8w6tkv3rra8rratnut36sk22vrm' address
    UNION ALL
    SELECT
        'mars-oracle' contract_name,
        'osmo1mhznfr60vjdp2gejhyv2gax9nvyyzhd3z0qcwseyetkfustjauzqycsy2g' address
    UNION ALL
    SELECT
        'mars-red-bank' contract_name,
        'osmo1c3ljch9dfw5kf52nfwpxd2zmj2ese7agnx0p9tenkrryasrle5sqf3ftpg' address
    UNION ALL
    SELECT
        'mars-rewards-collector' contract_name,
        'osmo1urvqe5mw00ws25yqdd4c4hlh8kdyf567mpcml7cdve9w08z0ydcqvsrgdy' address
    UNION ALL
    SELECT
        'mars-liquidation-filterer' contract_name,
        'osmo1v0ezrc0f7l5tlw6ws2r8kalhmvrfhe0fywrmj8kjgrwvud5r83uqywn66c' address
),
tx AS (
    SELECT
        DISTINCT A.block_timestamp,
        A.tx_id,
        A.msg_index,
        b.contract_name
    FROM
        {{ ref('silver__msg_attributes') }} A
        JOIN mars_contracts b
        ON A.attribute_value = b.address
    WHERE
        block_timestamp :: DATE >= '2023-02-07'
        AND attribute_key = '_contract_address'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) _inserted_timestamp
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_id,
    A.block_timestamp,
    A.tx_id,
    tx_succeeded,
    A.msg_group,
    A.msg_sub_group,
    A.msg_index,
    contract_name,
    OBJECT_AGG(
        attribute_key :: STRING,
        attribute_value :: variant
    ) AS attributes,
    attributes :action :: STRING AS action,
    attributes :user :: STRING AS USER,
    attributes :sender :: STRING AS sender,
    attributes :recipient :: STRING AS recipient,
    attributes :on_behalf_of :: STRING AS on_behalf_of,
    attributes :"to" :: STRING AS to_address,
    attributes :asset_index :: STRING AS asset_index,
    attributes :denom :: STRING AS denom,
    attributes :rewards_accrued :: FLOAT AS rewards_accrued,
    attributes :amount :: STRING AS amount,
    attributes :amount_scaled :: STRING AS amount_scaled,
    attributes :mars_rewards :: STRING AS mars_rewards,
    attributes :collateral_amount :: INT AS collateral_amount,
    attributes :collateral_amount_scaled :: INT AS collateral_amount_scaled,
    attributes :collateral_denom :: STRING AS collateral_denom,
    attributes :debt_amount :: INT AS debt_amount,
    attributes :debt_amount_scaled :: INT AS debt_amount_scaled,
    attributes :debt_denom :: STRING AS debt_denom,
    attributes :liquidator :: STRING AS liquidator,
    attributes :borrow_index :: STRING AS borrow_index,
    attributes :borrow_rate :: FLOAT AS borrow_rate,
    attributes :liquidity_index :: STRING AS liquidity_index,
    attributes :liquidity_rate :: FLOAT AS liquidity_rate,
    _inserted_timestamp
FROM
    {{ ref('silver__msg_attributes') }} A
    JOIN tx b
    ON A.tx_id = b.tx_id
    AND A.block_timestamp :: DATE = b.block_timestamp :: DATE
    AND A.msg_index = b.msg_index
WHERE
    A.block_timestamp :: DATE >= '2023-02-07'
    AND msg_type = 'wasm'
    AND attribute_key <> '_contract_address'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) _inserted_timestamp
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    block_id,
    A.block_timestamp,
    A.tx_id,
    tx_succeeded,
    A.msg_group,
    A.msg_sub_group,
    A.msg_index,
    contract_name,
    _inserted_timestamp
HAVING
    action IN (
        'deposit',
        'borrow',
        'repay',
        'claim_rewards',
        'balance_change',
        'withdraw',
        'distribute_rewards'
    )
