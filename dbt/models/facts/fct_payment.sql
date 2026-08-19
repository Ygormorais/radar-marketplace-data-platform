with payments as (
    select * from {{ ref('stg_payments') }}
), orders as (
    select * from {{ ref('stg_orders') }}
), customer_map as (
    select customer_id, customer_unique_id from {{ ref('stg_customers') }}
)
select
    {{ surrogate_key(["'payment'", 'p.order_id', 'p.payment_sequence']) }} as payment_key,
    p.order_id,
    p.payment_sequence,
    coalesce(dc.customer_key, -1) as customer_key,
    {{ date_key('o.purchased_at') }} as purchase_date_key,
    p.payment_type,
    p.installments,
    p.payment_amount,
    o.purchased_at
from payments as p
inner join orders as o on p.order_id = o.order_id
left join customer_map as cm on o.customer_id = cm.customer_id
left join {{ ref('dim_customer') }} as dc
    on cm.customer_unique_id = dc.customer_unique_id
    and o.purchased_at >= dc.valid_from
    and (o.purchased_at < dc.valid_to or dc.valid_to is null)

