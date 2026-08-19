select seller_id
from {{ ref('dim_seller') }}
where seller_key <> -1 and is_current = 1
group by seller_id
having count(*) <> 1

