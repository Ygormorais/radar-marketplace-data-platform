select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_at,
    item_amount,
    freight_amount,
    gross_amount,
    _ingested_at
from {{ source('silver', 'silver_order_items') }}

