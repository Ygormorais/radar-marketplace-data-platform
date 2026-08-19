with sales as (
    select
        purchase_date_key as date_key,
        count(distinct order_id) as order_count,
        sum(item_amount) as item_revenue,
        sum(freight_amount) as freight_revenue,
        sum(gross_amount) as gmv,
        count(distinct case when is_delivered then order_id end) as delivered_order_count,
        count(distinct case when is_terminal_failure then order_id end) as failed_order_count,
        count(distinct case when is_delivered_on_time = 1 then order_id end) as on_time_order_count,
        count(distinct case when is_delivered_on_time = 0 then order_id end) as late_order_count
    from {{ ref('fct_order_item') }}
    group by purchase_date_key
), payments as (
    select purchase_date_key as date_key, sum(payment_amount) as payment_amount
    from {{ ref('fct_payment') }}
    group by purchase_date_key
), reviews as (
    select review_date_key as date_key, avg(cast(review_score as decimal(18,4))) as average_review_score
    from {{ ref('fct_review') }}
    group by review_date_key
)
select
    d.date_key,
    d.full_date,
    coalesce(s.order_count, 0) as order_count,
    coalesce(s.item_revenue, 0) as item_revenue,
    coalesce(s.freight_revenue, 0) as freight_revenue,
    coalesce(s.gmv, 0) as gmv,
    coalesce(p.payment_amount, 0) as payment_amount,
    coalesce(s.delivered_order_count, 0) as delivered_order_count,
    coalesce(s.failed_order_count, 0) as failed_order_count,
    coalesce(s.on_time_order_count, 0) as on_time_order_count,
    coalesce(s.late_order_count, 0) as late_order_count,
    r.average_review_score
from {{ ref('dim_date') }} as d
left join sales as s on d.date_key = s.date_key
left join payments as p on d.date_key = p.date_key
left join reviews as r on d.date_key = r.date_key

