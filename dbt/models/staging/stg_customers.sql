select customer_id, customer_unique_id, zip_code_prefix, city, state, _ingested_at
from {{ source('silver', 'silver_customers') }}

