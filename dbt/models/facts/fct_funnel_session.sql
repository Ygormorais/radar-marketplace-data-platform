select
    {{ surrogate_key(["'session'", 'session_id']) }} as session_key,
    session_id,
    user_id,
    {{ date_key('session_started_at') }} as session_date_key,
    session_started_at,
    session_ended_at,
    session_duration_seconds,
    event_count,
    distinct_product_count,
    has_product_view,
    has_add_to_cart,
    has_checkout,
    has_purchase,
    device_type,
    traffic_source
from {{ source('silver', 'silver_clickstream_sessions') }}

