with seller_month as (
    select
        seller_key,
        datefromparts(year(purchased_at), month(purchased_at), 1) as order_month,
        count(distinct order_id) as order_count,
        sum(gross_amount) as gmv,
        avg(cast(delivery_delay_days as decimal(18,4))) as average_delay_days
    from gold.fct_order_item
    group by seller_key, datefromparts(year(purchased_at), month(purchased_at), 1)
), ranked as (
    select
        *,
        dense_rank() over (partition by order_month order by gmv desc) as revenue_rank,
        sum(gmv) over (
            partition by seller_key order by order_month
            rows between 2 preceding and current row
        ) as rolling_3m_gmv,
        avg(average_delay_days) over (
            partition by seller_key order by order_month
            rows between 2 preceding and current row
        ) as rolling_3m_delay
    from seller_month
)
select * from ranked where revenue_rank <= 20;

