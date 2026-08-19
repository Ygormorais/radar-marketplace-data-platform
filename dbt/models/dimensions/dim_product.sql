with history as (
    select * from {{ source('silver', 'silver_products_history') }}
), modeled as (
    select
        {{ surrogate_key(["'product'", 'product_id', 'valid_from']) }} as product_key,
        {{ surrogate_key(["'category'", 'category_name']) }} as category_key,
        product_id,
        category_name,
        product_name_length,
        product_description_length,
        product_photos_quantity,
        weight_g,
        length_cm,
        height_cm,
        width_cm,
        volume_cm3,
        valid_from,
        valid_to,
        is_current,
        created_run_id,
        updated_run_id
    from history
)
select * from modeled
union all
select
    cast(-1 as bigint), cast(-1 as bigint), 'UNKNOWN', 'unknown',
    null, null, null, null, null, null, null, null,
    cast('1900-01-01' as datetime2), null, cast(1 as bit), 'system', 'system'

