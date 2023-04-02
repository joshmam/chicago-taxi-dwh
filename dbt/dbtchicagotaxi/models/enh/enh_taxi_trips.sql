-- purpose of enh: enhance the source data with calculated fields

{{
  config(
    materialized = 'view',
    )
}}

WITH src_taxi_trips AS(
    SELECT * FROM {{ ref('src_taxi_trips') }}
)

SELECT
    src_taxi_trips.*,

    (trip_distance_miles * 1.6) AS trip_distance_km,
    ROUND(trip_duration_secs/60, 2) AS trip_duration_mins,
    (total_amount - tips_amount) AS total_revenue_exc_tips,
    CASE WHEN tips_amount > 0 THEN 1 ELSE 0 END AS has_tip,
    CASE WHEN extras_amount > 0 THEN 1 ELSE 0 END AS has_extras,
    CASE WHEN trip_distance_miles < 4 THEN 1 ELSE 0 END AS is_short_trip
FROM
    src_taxi_trips