{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp::DATE'],
    tags = ['noncore'],
    enabled = false
) }}

WITH count_ids AS (

    SELECT
        tx_id,
        msg_group,
        COALESCE(
            msg_sub_group,
            -1
        ) AS msg_sub_group,
        COUNT(
            DISTINCT attribute_value
        ) AS id_count
    FROM
        {{ ref('silver__msg_attributes') }} A
    WHERE
        attribute_key IN (
            'period_lock_id',
            'lock_id'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    tx_id,
    msg_group,
    COALESCE(
        msg_sub_group,
        -1
    )
),
pre_base AS (
    SELECT
        DISTINCT A.tx_id,
        A.msg_group,
        A.msg_sub_group,
        CASE
            WHEN id_count > 1 THEN msg_index
        END msg_index_gp,
        id_count,
        _inserted_timestamp,
        attribute_key,
        attribute_value
    FROM
        {{ ref('silver__msg_attributes') }} A
        JOIN count_ids b
        ON A.tx_id = b.tx_id
        AND A.msg_group = b.msg_group
        AND COALESCE(
            A.msg_sub_group,
            -1
        ) = b.msg_sub_group
    WHERE
        attribute_key IN (
            'period_lock_id',
            'lock_id',
            'action'
        )
        AND msg_type <> 'superfluid_undelegate'
        AND A.tx_id NOT IN (
            '523CBB1403A90A2A45A90ADFFC17F72100B99C286BD66DEDF22DD7F8A825127D',
            'B26B72516A670B4FFD31F4F7853E65F7463F7A46BDE61800DC17A41F55AB87A3',
            '34A6CEF2A87D6DB15DA1D7238D3A3BEABF8B4A1B460082B3C1F6C65DE5329CAC',
            '504A0BD295DA63E28D55BC7C46575C6C49D9C2612D0AF118BA2A33A089A25A6D',
            'B312127A7914D26444DA2C1104122F9CB7D3B50940F079544775C7EA4EE4981D',
            '413991DF25FF3A217BA42D84D811CABC4A580F12FA9A8BC204E45F22529185CB'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
),
base AS (
    SELECT
        tx_id,
        msg_group,
        msg_sub_group,
        msg_index_gp,
        id_count,
        _inserted_timestamp,
        OBJECT_AGG(
            CASE
                WHEN attribute_key = 'action' THEN attribute_key || (
                    SELECT
                        UUID_STRING()
                )
                ELSE attribute_key
            END :: STRING,
            attribute_value :: variant
        ) AS j,
        COALESCE(
            j :lock_id,
            j :period_lock_id
        ) :: INT AS lock_id
    FROM
        pre_base
    GROUP BY
        tx_id,
        msg_group,
        msg_sub_group,
        msg_index_gp,
        id_count,
        _inserted_timestamp
)
SELECT
    tx_id,
    msg_group,
    msg_index_gp AS msg_index,
    lock_id,
    id_count,
    _inserted_timestamp,
    concat_ws(
        '-',
        tx_id,
        msg_group,
        lock_id
    ) AS _unique_key,
    {{ dbt_utils.generate_surrogate_key(
        ['_unique_key']
    ) }} AS locked_liquidity_transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base
WHERE
    (
        j :: STRING LIKE '%action%'
        OR id_count > 1
    )
    AND lock_id IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY tx_id, COALESCE(msg_group, -1), lock_id
ORDER BY
    msg_index_gp DESC) = 1)
