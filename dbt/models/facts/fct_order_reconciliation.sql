with reconciliation as (
    select * from {{ source('silver', 'silver_financial_reconciliation') }}
)
select
    {{ surrogate_key(["'reconciliation'", 'order_id']) }} as reconciliation_key,
    order_id,
    {{ date_key('purchased_at') }} as purchase_date_key,
    order_status,
    purchased_at,
    item_amount,
    freight_amount,
    expected_payment_amount,
    actual_payment_amount,
    payment_difference,
    item_count,
    seller_count,
    payment_record_count,
    payment_method_count,
    is_payment_reconciled,
    reconciliation_status
from reconciliation

