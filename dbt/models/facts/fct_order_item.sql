with items as (
    select * from {{ ref('stg_order_items') }}
), orders as (
    select * from {{ ref('stg_orders') }}
), customer_map as (
    select customer_id, customer_unique_id from {{ ref('stg_customers') }}
), enriched as (
    select
        {{ surrogate_key(["'order_item'", 'i.order_id', 'i.order_item_id']) }} as order_item_key,
        i.order_id,
        i.order_item_id,
        coalesce(dc.customer_key, -1) as customer_key,
        coalesce(ds.seller_key, -1) as seller_key,
        coalesce(dp.product_key, -1) as product_key,
        coalesce(dg.geography_key, -1) as customer_geography_key,
        {{ date_key('o.purchased_at') }} as purchase_date_key,
        case o.order_status
            when 'created' then 10 when 'approved' then 20 when 'invoiced' then 30
            when 'processing' then 40 when 'shipped' then 50 when 'delivered' then 60
            when 'unavailable' then 70 when 'canceled' then 80 else -1
        end as order_status_key,
        o.purchased_at,
        o.delivered_at,
        o.estimated_delivery_at,
        o.delivery_lead_days,
        o.delivery_delay_days,
        o.is_delivered_on_time,
        o.is_delivered,
        o.is_terminal_failure,
        i.shipping_limit_at,
        i.item_amount,
        i.freight_amount,
        i.gross_amount
    from items as i
    inner join orders as o on i.order_id = o.order_id
    left join customer_map as cm on o.customer_id = cm.customer_id
    left join {{ ref('dim_customer') }} as dc
        on cm.customer_unique_id = dc.customer_unique_id
        and o.purchased_at >= dc.valid_from
        and (o.purchased_at < dc.valid_to or dc.valid_to is null)
    left join {{ ref('dim_seller') }} as ds
        on i.seller_id = ds.seller_id
        and o.purchased_at >= ds.valid_from
        and (o.purchased_at < ds.valid_to or ds.valid_to is null)
    left join {{ ref('dim_product') }} as dp
        on i.product_id = dp.product_id
        and o.purchased_at >= dp.valid_from
        and (o.purchased_at < dp.valid_to or dp.valid_to is null)
    left join {{ ref('dim_geography') }} as dg on dc.zip_code_prefix = dg.zip_code_prefix
)
select * from enriched

