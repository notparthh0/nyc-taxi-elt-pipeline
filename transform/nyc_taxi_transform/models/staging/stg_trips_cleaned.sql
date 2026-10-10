-- Staging model: reads directly from the cleaned Parquet table in Athena
-- Source: s3://nyc-taxi-de-parth-2026/cleaned/

SELECT
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    pulocationid,
    dolocationid,
    fare_amount,
    tip_amount,
    total_amount,

    -- Derived time features
    DATE(tpep_pickup_datetime)                          AS pickup_date,
    HOUR(tpep_pickup_datetime)                          AS pickup_hour,
    DAY_OF_WEEK(tpep_pickup_datetime)                   AS pickup_day_of_week,
    MONTH(tpep_pickup_datetime)                         AS pickup_month,

    -- Trip duration in minutes
    DATE_DIFF('minute', tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_duration_mins

FROM nyc_taxi.trips_cleaned

WHERE
    fare_amount > 0
    AND trip_distance > 0
    AND passenger_count > 0
    AND tpep_pickup_datetime IS NOT NULL
