select
    purchase_date_key as date_key,
    carrier_code,
    location_state,
    count(*) as order_count,
    sum(case when is_at_risk = 1 then 1 else 0 end) as at_risk_order_count,
    sum(case when is_delivered_on_time = 1 then 1 else 0 end) as on_time_order_count,
    sum(case when is_delivered_on_time = 0 then 1 else 0 end) as late_order_count,
    avg(cast(delivery_lead_days as decimal(18,4))) as average_delivery_lead_days,
    avg(cast(delivery_delay_days as decimal(18,4))) as average_delivery_delay_days,
    sum(coalesce(invalid_transition_count, 0)) as invalid_transition_count
from {{ ref('fct_delivery') }}
group by purchase_date_key, carrier_code, location_state

