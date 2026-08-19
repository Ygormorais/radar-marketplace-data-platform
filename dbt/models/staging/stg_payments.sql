select
    order_id,
    payment_sequence,
    payment_type,
    installments,
    payment_amount,
    _ingested_at
from {{ source('silver', 'silver_payments') }}

