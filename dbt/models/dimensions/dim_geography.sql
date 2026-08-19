with geography as (
    select * from {{ source('silver', 'silver_geography') }}
)
select
    {{ surrogate_key(["'geography'", 'zip_code_prefix']) }} as geography_key,
    zip_code_prefix,
    latitude,
    longitude,
    city,
    state,
    source_coordinate_count
from geography
union all
select cast(-1 as bigint), null, null, null, 'unknown', 'NA', 0

