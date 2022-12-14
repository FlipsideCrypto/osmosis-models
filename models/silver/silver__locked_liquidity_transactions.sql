{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH count_ids AS (

    SELECT
        tx_id,
        msg_group,
        msg_sub_group,
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
    msg_sub_group
),
base AS (
    SELECT
        A.tx_id,
        A.msg_group,
        A.msg_sub_group,
        CASE
            WHEN id_count > 1 THEN msg_index
        END msg_index_gp,
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
        {{ ref('silver__msg_attributes') }} A
        JOIN count_ids b
        ON A.tx_id = b.tx_id
        AND A.msg_group = b.msg_group
        AND A.msg_sub_group = b.msg_sub_group
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
GROUP BY
    A.tx_id,
    A.msg_group,
    A.msg_sub_group,
    msg_index_gp,
    id_count,
    _inserted_timestamp
)
SELECT
    tx_id,
    msg_group,
    lock_id,
    _inserted_timestamp,
    concat_ws(
        '-',
        tx_id,
        COALESCE(
            msg_group,
            -1
        ),
        lock_id
    ) AS _unique_key
FROM
    base
WHERE
    (
        j :: STRING LIKE '%action%'
        OR id_count > 1
    )
    AND lock_id IS NOT NULL
