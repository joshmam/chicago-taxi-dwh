{{
  config(
    materialized = 'incremental',
    on_schema_change='fail'
    )
}}

WITH enh_taxi_trips AS(
    SELECT * FROM {{ ref('enh_taxi_trips') }}
),
latest_row AS(
    SELECT
        TRIP_START_TIMESTAMP AS ts,
        TRIP_ID AS id
    FROM {{ this }}
    ORDER BY TRIP_START_TIMESTAMP DESC, TRIP_ID DESC
    LIMIT 1
)

SELECT
    TRIP_ID,
    TAXI_ID, 
    TRIP_DATE, 
    TRIP_START_TIMESTAMP, 
    TRIP_END_TIMESTAMP, 
    PICKUP_COMMUNITY_AREA, 
    DROPOFF_COMMUNITY_AREA, 
    PAYMENT_TYPE, 
    PICKUP_CENTROID_LATITUDE, 
    PICKUP_CENTROID_LONGITUDE, 
    DROPOFF_CENTROID_LATITUDE, 
    DROPOFF_CENTROID_LONGITUDE, 
    HAS_TIP, 
    HAS_EXTRAS,
    IS_SHORT_TRIP,

    TRIP_DURATION_SECS,
    TRIP_DURATION_MINS,
    TRIP_DISTANCE_MILES,
    TRIP_DISTANCE_KM,
    FARE_AMOUNT, 
    TIPS_AMOUNT, 
    TOLLS_AMOUNT, 
    EXTRAS_AMOUNT, 
    TOTAL_AMOUNT,
    TOTAL_REVENUE_EXC_TIPS

FROM enh_taxi_trips

-- to do: simplify by moving repeated code inside is_incremental() to a macro
{% if is_incremental() %}

WHERE

  ( TRIP_START_TIMESTAMP > (select ts from latest_row) )
  OR (
        ( TRIP_START_TIMESTAMP = (select ts from latest_row) )
        AND ( TRIP_ID > (select id from latest_row) ) 
    )


{% endif %}

ORDER BY TRIP_START_TIMESTAMP ASC, TRIP_ID ASC