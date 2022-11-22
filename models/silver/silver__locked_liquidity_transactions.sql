{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

SELECT
    DISTINCT tx_id,
    msg_group,
    msg_sub_group,
    CASE
        WHEN msg_type = 'begin_unlock' THEN msg_index
    END msg_index,
    attribute_value AS lock_id,
    _inserted_timestamp,
    concat_ws(
        '-',
        A.tx_id,
        COALESCE(
            A.msg_group,
            -1
        ),
        COALESCE(
            A.msg_sub_group,
            -1
        )
    ) AS _unique_key
FROM
    {{ ref('silver__msg_attributes') }} A
WHERE
    attribute_key IN (
        'period_lock_id',
        'lock_id'
    )
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
