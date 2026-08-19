with delayed as (
    select
        carrier_code,
        cast(delivered_at as date) as delivered_date,
        row_number() over (
            partition by carrier_code order by cast(delivered_at as date)
        ) as sequence_number
    from gold.fct_delivery
    where is_delivered_on_time = 0 and delivered_at is not null
    group by carrier_code, cast(delivered_at as date)
), islands as (
    select
        *,
        dateadd(day, -sequence_number, delivered_date) as island_key
    from delayed
)
select
    carrier_code,
    min(delivered_date) as delay_streak_started_at,
    max(delivered_date) as delay_streak_ended_at,
    count(*) as consecutive_delay_days
from islands
group by carrier_code, island_key
having count(*) >= 3;

