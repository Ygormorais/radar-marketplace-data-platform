with history as (
    select * from {{ source('silver', 'silver_sellers_history') }}
), modeled as (
    select
        {{ surrogate_key(["'seller'", 'seller_id', 'valid_from']) }} as seller_key,
        seller_id,
        zip_code_prefix,
        city,
        state,
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
    cast(-1 as bigint), 'UNKNOWN', null, 'unknown', 'NA',
    cast('1900-01-01' as datetime2), null, cast(1 as bit), 'system', 'system'

