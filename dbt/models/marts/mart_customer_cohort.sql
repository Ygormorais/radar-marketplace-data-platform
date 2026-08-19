with customer_orders as (
    select distinct customer_key, order_id, cast(purchased_at as date) as purchased_date
    from {{ ref('fct_order_item') }}
    where customer_key <> -1 and is_terminal_failure = 0
), first_purchase as (
    select customer_key, min(purchased_date) as cohort_date
    from customer_orders
    group by customer_key
), activity as (
    select
        o.customer_key,
        datefromparts(year(f.cohort_date), month(f.cohort_date), 1) as cohort_month,
        datediff(
            month,
            datefromparts(year(f.cohort_date), month(f.cohort_date), 1),
            datefromparts(year(o.purchased_date), month(o.purchased_date), 1)
        ) as months_since_first_purchase
    from customer_orders as o
    inner join first_purchase as f on o.customer_key = f.customer_key
)
select
    cohort_month,
    months_since_first_purchase,
    count(distinct customer_key) as retained_customer_count
from activity
group by cohort_month, months_since_first_purchase

