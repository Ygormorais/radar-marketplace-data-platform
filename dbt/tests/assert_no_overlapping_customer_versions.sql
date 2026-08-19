with versions as (
    select
        customer_unique_id,
        valid_from,
        valid_to,
        lag(valid_to) over (
            partition by customer_unique_id
            order by valid_from
        ) as previous_valid_to
    from {{ ref('dim_customer') }}
    where customer_key <> -1
)
select *
from versions
where previous_valid_to > valid_from

