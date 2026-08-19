with digits as (
    select n
    from (values (0), (1), (2), (3), (4), (5), (6), (7), (8), (9)) as valueset(n)
), numbers as (
    select
        d0.n + d1.n * 10 + d2.n * 100 + d3.n * 1000 + d4.n * 10000 as n
    from digits as d0
    cross join digits as d1
    cross join digits as d2
    cross join digits as d3
    cross join digits as d4
), spine as (
    select dateadd(day, n, cast('{{ var("gold_start_date") }}' as date)) as full_date
    from numbers
    where dateadd(day, n, cast('{{ var("gold_start_date") }}' as date))
        <= cast('{{ var("gold_end_date") }}' as date)
)
select
    {{ date_key('full_date') }} as date_key,
    full_date,
    datepart(year, full_date) as calendar_year,
    datepart(quarter, full_date) as calendar_quarter,
    datepart(month, full_date) as calendar_month,
    datepart(day, full_date) as day_of_month,
    datepart(dayofyear, full_date) as day_of_year,
    datediff(day, '19000101', full_date) % 7 + 1 as iso_day_of_week,
    dateadd(day, -(datediff(day, '19000101', full_date) % 7), full_date) as week_started_at,
    eomonth(full_date) as month_ended_at,
    case when datediff(day, '19000101', full_date) % 7 in (5, 6) then 1 else 0 end as is_weekend
from spine

