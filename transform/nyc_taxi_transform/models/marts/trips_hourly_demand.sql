-- Mart model: hourly demand aggregation by pickup location
-- This is the direct input for the XGBoost demand prediction model

SELECT
    pickup_date,
    pickup_hour,
    pickup_day_of_week,
    pickup_month,
    pulocationid,

    -- Demand metrics
    COUNT(*)                        AS trip_count,
    AVG(trip_distance)              AS avg_trip_distance,
    AVG(fare_amount)                AS avg_fare,
    AVG(passenger_count)            AS avg_passengers,
    AVG(trip_duration_mins)         AS avg_duration_mins,
    SUM(total_amount)               AS total_revenue

FROM {{ ref('stg_trips_cleaned') }}

GROUP BY
    pickup_date,
    pickup_hour,
    pickup_day_of_week,
    pickup_month,
    pulocationid

ORDER BY
    pickup_date,
    pickup_hour,
    pulocationid
