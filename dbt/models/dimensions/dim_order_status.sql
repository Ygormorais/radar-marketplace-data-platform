select *
from (
    values
        (10, 'created', 'open'),
        (20, 'approved', 'open'),
        (30, 'invoiced', 'open'),
        (40, 'processing', 'open'),
        (50, 'shipped', 'in_transit'),
        (60, 'delivered', 'success'),
        (70, 'unavailable', 'failure'),
        (80, 'canceled', 'failure'),
        (-1, 'unknown', 'unknown')
) as status_values(order_status_key, order_status, status_group)

