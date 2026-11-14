-- Staging: one row per cleaned trip, plus calendar features used downstream.
-- The SQL runs unchanged on Athena and DuckDB; the two functions that differ
-- between them are wrapped in macros (macros/cross_db.sql).

with source as (

    select * from {{ source('nyc_taxi', 'trips_cleaned') }}

)

-- explicit lower-case aliases: DuckDB keeps the Parquet spelling (PULocationID),
-- Athena lower-cases everything; this makes both engines return the same names
select
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    pulocationid                                                      as pulocationid,
    dolocationid                                                      as dolocationid,
    fare_amount,
    tip_amount,
    total_amount,

    -- calendar features (known before any trip happens)
    cast(tpep_pickup_datetime as date)                                as pickup_date,
    hour(tpep_pickup_datetime)                                        as pickup_hour,
    {{ iso_day_of_week('tpep_pickup_datetime') }}                     as pickup_day_of_week,  -- 1 = Monday
    month(tpep_pickup_datetime)                                       as pickup_month,

    date_diff('minute', tpep_pickup_datetime, tpep_dropoff_datetime)  as trip_duration_mins,

    -- 'YYYY-MM': the unit of incremental loading
    {{ year_month('tpep_pickup_datetime') }}                          as month_key

from source
where tpep_pickup_datetime is not null
