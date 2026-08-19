with delivery as (
    select * from {{ source('silver', 'silver_delivery_snapshot') }}
), orders as (
    select * from {{ ref('stg_orders') }}
)
select
    {{ surrogate_key(["'delivery'", 'd.order_id']) }} as delivery_key,
    d.order_id,
    {{ date_key('o.purchased_at') }} as purchase_date_key,
    case when o.delivered_at is null then null else {{ date_key('o.delivered_at') }} end
        as delivered_date_key,
    o.order_status,
    d.latest_delivery_status,
    d.latest_event_at,
    d.latest_event_received_at,
    d.location_state,
    d.carrier_code,
    o.purchased_at,
    o.estimated_delivery_at,
    o.delivered_at,
    o.delivery_lead_days,
    o.delivery_delay_days,
    o.is_delivered_on_time,
    d.is_at_risk,
    d.invalid_transition_count,
    d.delivery_event_count,
    d.event_arrival_lag_minutes
from delivery as d
inner join orders as o on d.order_id = o.order_id

