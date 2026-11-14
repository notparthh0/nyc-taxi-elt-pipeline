-- Mart: trips per pickup zone per hour. Input to the XGBoost model and the dashboard.
--
-- Incremental by month: with --vars '{"month": "2023-02"}' only that month is
-- rebuilt and swapped in, so re-running a month never duplicates rows.
--   Athena -> insert_overwrite on the month_key partition
--   DuckDB -> delete+insert on month_key
-- `dbt build --full-refresh` rebuilds every month from scratch.

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite' if target.type == 'athena' else 'delete+insert',
    unique_key = none if target.type == 'athena' else 'month_key',
    partitioned_by = ['month_key'] if target.type == 'athena' else none,
    on_schema_change = 'fail'
) }}

select
    pickup_date,
    pickup_hour,
    pickup_day_of_week,
    pickup_month,
    pulocationid,

    count(*)                  as trip_count,
    avg(trip_distance)        as avg_trip_distance,
    avg(fare_amount)          as avg_fare,
    avg(passenger_count)      as avg_passengers,
    avg(trip_duration_mins)   as avg_duration_mins,
    sum(total_amount)         as total_revenue,

    month_key                 -- partition column; must stay last for Athena

from {{ ref('stg_trips_cleaned') }}

{% if is_incremental() and var('month') %}
where month_key = '{{ var("month") }}'
{% endif %}

group by
    pickup_date,
    pickup_hour,
    pickup_day_of_week,
    pickup_month,
    pulocationid,
    month_key
