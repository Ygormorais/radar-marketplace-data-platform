select
    seller_key,
    count(distinct order_id) as order_count,
    sum(item_amount) as item_revenue,
    sum(freight_amount) as freight_revenue,
    sum(gross_amount) as gmv,
    avg(cast(delivery_delay_days as decimal(18,4))) as average_delay_days,
    cast(
        count(distinct case when is_delivered_on_time = 1 then order_id end) * 1.0
        / nullif(count(distinct case when is_delivered = 1 then order_id end), 0)
        as decimal(9,6)
    ) as on_time_delivery_rate,
    max(purchased_at) as last_order_at
from {{ ref('fct_order_item') }}
group by seller_key

