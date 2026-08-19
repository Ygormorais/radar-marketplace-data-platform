with funnel as (
    with daily as (
        select
            session_date_key,
            count(*) as sessions,
            sum(case when has_add_to_cart = 1 then 1 else 0 end) as carts,
            sum(case when has_purchase = 1 then 1 else 0 end) as purchases
        from gold.fct_funnel_session
        group by session_date_key
    )
    select
        *,
        cast(carts * 1.0 / nullif(sessions, 0) as decimal(9,6)) as cart_rate,
        cast(purchases * 1.0 / nullif(carts, 0) as decimal(9,6)) as cart_to_purchase_rate
    from daily
)
select * from funnel;

