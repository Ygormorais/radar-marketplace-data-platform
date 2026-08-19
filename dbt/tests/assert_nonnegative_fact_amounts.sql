select order_item_key, item_amount, freight_amount
from {{ ref('fct_order_item') }}
where item_amount < 0 or freight_amount < 0

