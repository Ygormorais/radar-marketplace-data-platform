with reviews as (
    select * from {{ ref('stg_reviews') }}
), orders as (
    select * from {{ ref('stg_orders') }}
)
select
    {{ surrogate_key(["'review'", 'r.review_id', 'r.order_id']) }} as review_key,
    r.review_id,
    r.order_id,
    {{ date_key('r.review_created_at') }} as review_date_key,
    r.review_score,
    r.review_title,
    r.review_message,
    r.review_created_at,
    r.review_answered_at,
    o.delivery_delay_days,
    o.is_delivered_on_time
from reviews as r
inner join orders as o on r.order_id = o.order_id

