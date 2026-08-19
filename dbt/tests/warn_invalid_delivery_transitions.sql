{{ config(severity='warn') }}

select order_id, invalid_transition_count
from {{ ref('fct_delivery') }}
where coalesce(invalid_transition_count, 0) > 0

