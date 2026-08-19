select
    review_id,
    order_id,
    review_score,
    review_title,
    review_message,
    review_created_at,
    review_answered_at,
    _ingested_at
from {{ source('silver', 'silver_reviews') }}

