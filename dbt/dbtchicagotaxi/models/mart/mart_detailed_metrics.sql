-- purpose: mart table with key metrics by day with additional dimensions for more detailed reporting

{{
  config(
    materialized = 'table',
    )
}}

WITH fct_taxi_trips AS(
    SELECT * FROM {{ ref('fct_taxi_trips') }}
),
dim_taxis AS(
    SELECT * FROM {{ ref('dim_taxis') }}
),
dim_dates AS(
    SELECT * FROM {{ ref('dimdates') }}
),
weather AS (
    SELECT * FROM {{ ref('chicago_weather_history') }}
),



agg_daily_trips AS(
    SELECT
     trip_date AS date,

     payment_type,
     taxi_id,
     pickup_community_area,
     dropoff_community_area,
     is_short_trip,
     has_tip,

     COUNT(tt.taxi_id) AS trip_count,
     SUM(tt.trip_distance_km) AS total_trip_distance_km,
     SUM(tt.trip_duration_mins) AS trip_duration_mins,
     SUM(tt.fare_amount) AS fares_amount,
     SUM(tt.tips_amount) AS tips_amount,
     SUM(tt.total_revenue_exc_tips) AS total_revenue_exc_tip

    FROM fct_taxi_trips tt
    GROUP BY date, payment_type, taxi_id, pickup_community_area, dropoff_community_area, is_short_trip, has_tip
)

SELECT
    CAST(d.yearnum AS varchar) AS year,
    d.monthshortname AS month,
    d.monthnum AS month_num,
    d.dayname AS day,

    t.*,

    taxis.company AS company,

    w.tempmax AS weather_daily_temp_high,
    w.tempmin AS weather_daily_temp_low
    
FROM agg_daily_trips t
LEFT JOIN dim_taxis taxis
ON taxis.taxi_id = t.taxi_id
LEFT JOIN dim_dates d
ON d.date = t.date
LEFT JOIN weather w
ON w.datetime = t.date
ORDER BY year, monthnum