select
    session_date_key as date_key,
    device_type,
    traffic_source,
    count(*) as session_count,
    sum(case when has_product_view = 1 then 1 else 0 end) as product_view_sessions,
    sum(case when has_add_to_cart = 1 then 1 else 0 end) as cart_sessions,
    sum(case when has_checkout = 1 then 1 else 0 end) as checkout_sessions,
    sum(case when has_purchase = 1 then 1 else 0 end) as purchase_sessions
from {{ ref('fct_funnel_session') }}
group by session_date_key, device_type, traffic_source

