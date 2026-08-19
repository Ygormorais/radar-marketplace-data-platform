with categories as (
    select distinct coalesce(category_name, 'unknown') as category_name
    from {{ source('silver', 'silver_products_history') }}
)
select
    {{ surrogate_key(["'category'", 'category_name']) }} as category_key,
    category_name
from categories
union all
select cast(-1 as bigint), 'unknown'

