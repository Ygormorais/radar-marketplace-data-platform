select
    order_id,
    customer_id,
    order_status,
    purchased_at,
    approved_at,
    handed_to_carrier_at,
    delivered_at,
    estimated_delivery_at,
    approval_lead_hours,
    delivery_lead_days,
    delivery_delay_days,
    is_delivered_on_time,
    is_delivered,
    is_terminal_failure,
    _ingested_at
from {{ source('silver', 'silver_orders') }}

