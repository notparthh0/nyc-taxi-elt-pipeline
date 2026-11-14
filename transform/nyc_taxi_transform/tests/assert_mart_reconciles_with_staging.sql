-- Reconciliation: every cleaned trip must be counted exactly once in the hourly mart.
-- Returns one row per month where the two totals disagree (so 0 rows = pass).

with staged as (

    select month_key, count(*) as trips
    from {{ ref('stg_trips_cleaned') }}
    group by month_key

),

mart as (

    select month_key, sum(trip_count) as trips
    from {{ ref('trips_hourly_demand') }}
    group by month_key

)

select
    coalesce(staged.month_key, mart.month_key) as month_key,
    staged.trips                               as staged_trips,
    mart.trips                                 as mart_trips
from staged
full outer join mart
    on staged.month_key = mart.month_key
where staged.trips is null
   or mart.trips is null
   or staged.trips <> mart.trips
