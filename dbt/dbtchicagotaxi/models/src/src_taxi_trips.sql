-- purpose of src: filter, clean, and parse the raw data

{{
  config(
    materialized = 'ephemeral',
    )
}}

WITH raw_taxi_trips AS(
    SELECT
        *
    FROM {{ source('chicago_raw', 'taxi_trips') }}
)

SELECT
    trip_id,
    taxi_id,

    CAST(SUBSTR(trip_start_timestamp, 1, 19) AS DATE) AS trip_date,
    CAST(SUBSTR(trip_start_timestamp, 1, 19) AS TIMESTAMP) AS trip_start_timestamp,
    CAST(SUBSTR(trip_end_timestamp, 1, 19) AS TIMESTAMP) AS trip_end_timestamp,
    
    trip_seconds AS trip_duration_secs,
    trip_miles AS trip_distance_miles,
    NVL(pickup_community_area, -1) AS pickup_community_area,
    NVL(dropoff_community_area, -1) AS dropoff_community_area,
    fare AS fare_amount,
    tips AS tips_amount,
    tolls AS tolls_amount,
    extras AS extras_amount,
    trip_total AS total_amount,
    payment_type,

    CASE
        WHEN company = 'Taxicab Insurance Agency Llc' THEN 'Taxicab Insurance Agency, LLC'
        WHEN company = 'Top Cab' THEN 'Top Cab Affiliation'
        ELSE company
    END AS company,

    pickup_centroid_latitude,
    pickup_centroid_longitude,
    dropoff_centroid_latitude,
    dropoff_centroid_longitude
FROM raw_taxi_trips