{{ config(
  materialized = 'view',
  post_hook = "call "
) }}

select 
    a.value::asset_address::string as address
from {{ ref('silver__pool_metadata') }},
table(flatten(assets)) a
except
select address
from {{ ref('silver__asset_metadata') }}